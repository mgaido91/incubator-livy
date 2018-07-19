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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.server.HiveServer2

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.InteractiveSessionManager

/**
 * The main entry point for the Livy thrift server leveraging HiveServer2. Starts up a
 * `HiveThriftServer2` thrift server.
 */
object LivyThriftServer extends Logging {

  private var thriftServerThread: Thread = _
  private val thriftserverThreadGroup = new ThreadGroup("thriftserver")
  /*
  def main(args: Array[String]) {

    val optionsProcessor = new HiveServer2.ServerOptionsProcessor("LivyThriftServer")
    optionsProcessor.parse(args)
    info("Starting LivyThriftServer")

    try {
      val sessionManager =
      val server = new LivyThriftServer()
      server.init(new HiveConf())
      server.start()
      info("LivyThriftServer started")
    } catch {
      case e: Exception =>
        error("Error starting LivyThriftServer", e)
        System.exit(-1)
    }
  }
  */

  def start(
      livyConf: LivyConf,
      livySessionManager: InteractiveSessionManager,
      sessionStore: SessionStore): Unit = synchronized {
    if (thriftServerThread == null) {
      info("Starting LivyThriftServer")
      val runThriftServer = new Runnable {
        override def run(): Unit = {
          try {
            val server = new LivyThriftServer(livyConf, livySessionManager, sessionStore)
            server.init(new HiveConf())
            server.start()
            info("LivyThriftServer started")
          } catch {
            case e: Exception =>
              error("Error starting LivyThriftServer", e)
          }
        }
      }
      thriftServerThread =
        new Thread(thriftserverThreadGroup, runThriftServer, "Livy-Thriftserver")
      thriftServerThread.start()
    } else {
      error("Livy Thriftserver is already started")
    }
  }
}


class LivyThriftServer(
      private[thriftserver] val livyConf: LivyConf,
      private[thriftserver] val livySessionManager: InteractiveSessionManager,
      private[thriftserver] val sessionStore: SessionStore) extends HiveServer2 {
  override def init(hiveConf: HiveConf): Unit = {
    this.cliService = new LivyCLIService(this)
    super.init(hiveConf)
  }
}
