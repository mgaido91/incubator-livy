package org.apache.livy.thriftserver

import java.security.PrivilegedExceptionAction
import java.util.concurrent.RejectedExecutionException
import java.util.{Arrays, Map => JMap}

import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.livy.Logging
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.thriftserver.rpc.RpcClient
import org.apache.livy.thriftserver.utils.HiveTypes

import scala.util.control.NonFatal

class LivyExecuteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true,
    livySession: InteractiveSession)
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
    with Logging {
  private val rpcClient = new RpcClient(livySession)

  private def statementId: String = getHandle.getHandleIdentifier.toString

  override def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(Arrays.asList(OperationState.FINISHED))
    setHasResultSet(true)

    // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
    val maxRows = maxRowsL.toInt
    rpcClient.fetchResult(statementId, maxRows).get().toRowSet
  }

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      execute()
    } else {
      val livyServiceUGI = Utils.getUGI

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException =>
                  setOperationException(e)
                  error("Error running hive query: ", e)
              }
            }
          }

          try {
            livyServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              error("Error running hive query as user : " +
                livyServiceUGI.getShortUserName, e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle =
          parentSession.getSessionManager.submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          error(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          throw e
      }
    }
  }

  protected def execute(): Unit = {
    info(s"Running query '$statement' with id $statementId")

    setState(OperationState.RUNNING)

    try {
      rpcClient.executeSql(parentSession.getSessionHandle, statementId, statement).get()
    } catch {
      case e: Throwable =>
        val currentState = getStatus.getState
        error(s"Error executing query, currentState $currentState, ", e)
        setState(OperationState.ERROR)
        throw new HiveSQLException(e.getMessage)
    }
    setState(OperationState.FINISHED)
  }

  def close(): Unit = {
    info(s"Close $statementId")
    cleanup(OperationState.CLOSED)
  }

  override def cancel(state: OperationState): Unit = {
    info(s"Cancel $statementId with state $state")
    cleanup(state)
  }

  def getResultSetSchema: TableSchema = {
    rpcClient.fetchResultSchema(statementId).get()
  }

  private def cleanup(state: OperationState) {
    if (statementId != null) {
      rpcClient.cleanupStatement(statementId)
    }
    setState(state)
  }
}
