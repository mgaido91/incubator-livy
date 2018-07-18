/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.thriftserver

import java.util.{Collections => JCollections, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Try}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.livy.Logging
import org.apache.livy.server.interactive.{CreateInteractiveRequest, InteractiveSession}
import org.apache.livy.sessions.Spark
import org.apache.livy.thriftserver.rpc.RpcClient

class LivyThriftSessionManager(val server: LivyThriftServer)
  extends SessionManager(server) with Logging {
  private val sessionHandleToLivySession =
    new ConcurrentHashMap[SessionHandle, InteractiveSession]()
  private val managedLivySessionActiveUsers =
    new mutable.HashMap[Int, Int]()

  def getLivySession(sessionHandle: SessionHandle): Option[InteractiveSession] = {
    Option(sessionHandleToLivySession.get(sessionHandle))
  }

  def onLivySessionCreated(livySession: InteractiveSession): Unit = {
    managedLivySessionActiveUsers.synchronized {
      val activeUsers = managedLivySessionActiveUsers.getOrElse(livySession.id, 0)
      managedLivySessionActiveUsers(livySession.id) = activeUsers + 1
    }
  }

  def onUserSessionClosed(sessionHandle: SessionHandle, livySession: InteractiveSession): Unit = {
    val closeSession = managedLivySessionActiveUsers.synchronized[Boolean] {
      val activeUsers = managedLivySessionActiveUsers(livySession.id)
      if (activeUsers == 1) {
        // it was the last user, so we can close the LivySession
        managedLivySessionActiveUsers -= livySession.id
        true
      } else {
        managedLivySessionActiveUsers(livySession.id) = activeUsers - 1
        false
      }
    }
    if (closeSession) {
      livySession.stopSession()
    } else {
      // We unregister the session only if we don't close it, as it is unnecessary in that case
      val rpcClient = new RpcClient(livySession)
      try {
        rpcClient.executeUnregisterSession(sessionHandle)
      } catch {
        case e: Exception => warn(s"Unable to unregister session $sessionHandle", e)
      }
    }
  }

  override def init(hiveConf: HiveConf): Unit = {
    operationManager = new LivyOperationManager(this)
    super.init(hiveConf)
  }

  private def getOrCreateLivySession(
      sessionHandle: SessionHandle,
      sessionId: Option[Int],
      username: String,
      createLivySession: () => InteractiveSession): InteractiveSession = {
    sessionId match {
      case Some(id) =>
        server.livySessionManager.get(id) match {
          case None =>
            warn(s"Session id $id doesn't exist, so we will ignore it.")
            createLivySession()
          case Some(session) if session.proxyUser.getOrElse(session.owner) != username =>
            warn(s"Session id $id doesn't belong to $username, so we will ignore it.")
            createLivySession()
          case Some(session) => if (session.state.isActive) {
            session
          } else {
            warn(s"Session id $id is not active anymore, so we will ignore it.")
            createLivySession()
          }
        }
      case None =>
        createLivySession()
    }
  }

  private def initSession(sessionHandle: SessionHandle, initStatements: List[String]): Unit = {
    val livySession = sessionHandleToLivySession.get(sessionHandle)
    val rpcClient = new RpcClient(livySession)
    rpcClient.executeRegisterSession(sessionHandle).get()
    val hiveSession = getSession(sessionHandle)
    initStatements.foreach { statement =>
      val operation = operationManager.newExecuteStatementOperation(
        hiveSession,
        statement,
        JCollections.emptyMap(),
        false,
        0)
      try {
        operation.run()
      } catch {
        case e: Exception => warn(s"Unable to run: $statement", e)
      } finally {
        operationManager.closeOperation(operation.getHandle)
      }
    }
  }

  override def openSession(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val sessionHandle = super.openSession(
      protocol, username, password, ipAddress, sessionConf, withImpersonation, delegationToken)
    val (initStatements, createInteractiveRequest, sessionId) =
      LivyThriftSessionManager.processSessionConf(sessionConf)
    val createLivySession = () => {
      createInteractiveRequest.kind = Spark
      val newSession = InteractiveSession.create(
        server.livySessionManager.nextId(),
        username,
        None,
        server.livyConf,
        createInteractiveRequest,
        server.sessionStore)
      onLivySessionCreated(newSession)
      newSession
    }
    val livySession = getOrCreateLivySession(sessionHandle, sessionId, username, createLivySession)
    sessionHandleToLivySession.put(sessionHandle, livySession)
    Try(initSession(sessionHandle, initStatements)) match {
      case Failure(e) =>
        warn(s"Init session $sessionHandle failed.", e)
        Try(closeSession(sessionHandle)).failed.foreach { e =>
          warn(s"Closing session $sessionHandle failed.", e)
        }
      case _ => // do nothing
    }
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    val removedSession = sessionHandleToLivySession.remove(sessionHandle)
    onUserSessionClosed(sessionHandle, removedSession)
  }
}

object LivyThriftSessionManager extends Logging {
  // Users can explicitly set the Livy connection id they want to connect to using this hiveconf
  // variable
  private val livySessionIdConfigKey = "set:hiveconf:livy.server.sessionId"
  private val livySessionConfRegexp = "set:hiveconf:livy.session.conf.(.*)".r

  private def convertConfValueToInt(key: String, value: String): Option[Int] = {
    val res = Try(value.toInt)
    if (res.isFailure) {
      warn(s"Ignoring $key = $value as it is not a valid integer")
      None
    } else {
      Some(res.get)
    }
  }

  private def processSessionConf(sessionConf: JMap[String, String]):
      (List[String], CreateInteractiveRequest, Option[Int]) = {
    if (null != sessionConf && !sessionConf.isEmpty) {
      val statements = new mutable.ListBuffer[String]
      val extraLivyConf = new mutable.ListBuffer[(String, String)]
      val createInteractiveRequest = new CreateInteractiveRequest
      sessionConf.asScala.foreach {
        case (key, value) =>
          key match {
            case v if v.startsWith("use:") => statements += s"use $value"
            // Process session configs for Livy session creation request
            case "set:hiveconf:livy.session.driverMemory" =>
              createInteractiveRequest.driverMemory = Some(value)
            case "set:hiveconf:livy.session.driverCores" =>
              createInteractiveRequest.driverCores = convertConfValueToInt(key, value)
            case "set:hiveconf:livy.session.executorMemory" =>
              createInteractiveRequest.executorMemory = Some(value)
            case "set:hiveconf:livy.session.executorCores" =>
              createInteractiveRequest.executorCores = convertConfValueToInt(key, value)
            case "set:hiveconf:livy.session.queue" =>
              createInteractiveRequest.queue = Some(value)
            case "set:hiveconf:livy.session.name" =>
              createInteractiveRequest.name = Some(value)
            case "set:hiveconf:livy.session.heartbeatTimeoutInSecond" =>
              convertConfValueToInt(key, value).foreach { heartbeatTimeoutInSecond =>
                createInteractiveRequest.heartbeatTimeoutInSecond = heartbeatTimeoutInSecond
              }
            case livySessionConfRegexp(livyConfKey) => extraLivyConf += (livyConfKey -> value)
            case _ =>
              info(s"Ignoring key: $key = '$value'")
          }
      }
      createInteractiveRequest.conf = extraLivyConf.toMap
      val sessionId = Option(sessionConf.get(livySessionIdConfigKey)).flatMap { id =>
        val res = Try(id.toInt)
        if (res.isFailure) {
          warn(s"Ignoring $livySessionIdConfigKey=$id as it is not an int.")
          None
        } else {
          Some(res.get)
        }
      }
      (statements.toList, createInteractiveRequest, sessionId)
    } else {
      (List(), new CreateInteractiveRequest, None)
    }
  }
}
