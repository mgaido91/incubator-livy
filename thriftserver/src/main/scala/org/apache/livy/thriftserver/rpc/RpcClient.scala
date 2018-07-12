package org.apache.livy.thriftserver.rpc

import org.apache.livy.{Job, JobContext, JobHandle, Logging}
import org.apache.livy.rsc.RSCClient
import org.apache.hive.service.cli.TableSchema


class RpcClient(val rscClient: RSCClient) extends Logging {

  private def getSparkEntry(jobContext: JobContext): AnyRef = {
    val majorSparkVersion = jobContext.sc().version.substring(0, 1)
    if (majorSparkVersion == "1") {
      Option(jobContext.hivectx()).getOrElse(jobContext.sqlctx())
    } else {
      jobContext.sparkSession()
    }
  }

  // private def runSql(spark: AnyRef, sql: String): DataFrame = {
  //  spark.getClass.getMethod("sql", classOf[String]).invoke(spark, sql).asInstanceOf[DataFrame]
  // }

  @throws[Exception]
  def executeSql(statementId: String, statement: String): JobHandle[_] = {
    info(s"RSC client is executing SQL query: $statement , statementId = $statementId")
    if (null == statementId || null == statement) {
      throw new IllegalArgumentException("Invalid statement/statementId specified. " +
        s"Statement = $statement, statementId = $statementId")
    }
    rscClient.submit(new Job[Boolean] {
      override def call(jc: JobContext): Boolean = {
        debug(s"SqlStatementRequest. statementId = $statementId, statement = $statement")
        jc.sc().setJobGroup(statementId, statement)
        val spark = jc.getSharedObject(sparkSessionSharedObjectName(parentSessionId))
        // val result = runSql(spark, statement)
        // TODO
        ???
      }
    })
  }

  @throws[Exception]
  def executeRegisterSession(userName: String, parentSessionId: String): JobHandle[_] = {
    info(s"RSC client is executing register session $parentSessionId")
    if (null == parentSessionId) {
      throw new IllegalArgumentException(s"Invalid parentSessionId = $parentSessionId")
    }
    rscClient.submit(new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {
        debug(s"RegisterSessionRequest. parentSessionId = $parentSessionId")
        if (RpcClient.SHARED_SINGLE_SESSION.equals(parentSessionId)) {
          jobContext.setSharedObject(sparkSessionSharedObjectName(parentSessionId),
            getSparkEntry(jobContext))
        } else {
          // create a new spark session / spark context for each client
          val spark = getSparkEntry(jobContext)
          jobContext.setSharedObject(sparkSessionSharedObjectName(parentSessionId),
            spark.getClass.getMethod("newInstance").invoke(spark))
        }
        true
      }
    })
  }

  @throws[Exception]
  def executeUnregisterSession(parentSessionId: String): JobHandle[_] = {
    info(s"RSC client is executing unregister session $parentSessionId")
    if (null == parentSessionId) {
      throw new IllegalArgumentException(s"Invalid parentSessionId = $parentSessionId")
    }
    //rscClient.submit(RemoteDriver.createUnregisterSessionRequest(parentSessionId))
    //TODO
    ???
  }

  @throws[Exception]
  def fetchResult(statementId: String, maxRows: Int): JobHandle[ResultSetWrapper] = {
    info(s"RSC client is fetching result for statementId = $statementId")
    if (null == statementId) {
      throw new IllegalArgumentException(
        s"Invalid statementId specified. StatementId = $statementId")
    }
    //rscClient.submit(RemoteDriver.createFetchQueryOutputRequest(statementId, maxRows))
    //TODO
    ???
  }

  @throws[Exception]
  def fetchResultSchema(statementId: String): JobHandle[TableSchema] = {
    info(s"RSC client is fetching result schema for statementId = $statementId")
    if (null == statementId) {
      throw new IllegalArgumentException(
        s"Invalid statementId specified. statementId = $statementId")
    }
    //rscClient.submit(RemoteDriver.createFetchResultSchemaRequest(statementId))
    //TODO
    ???
  }

  @throws[Exception]
  def cancelStatement(statementId: String): JobHandle[_] = {
    info(s"RSC client is canceling SQL query for statementId = $statementId")
    if (null == statementId) {
      throw new IllegalArgumentException(
        s"Invalid statementId specified. statementId = $statementId")
    }
    //rscClient.submit(RemoteDriver.createCancelStatementRequest(statementId))
    //TODO
    ???
  }

  @throws[Exception]
  def closeOperation(statementId: String): JobHandle[_] = {
    info(s"RSC client is closing operation for statementId = $statementId")
    //rscClient.submit(RemoteDriver.createCloseOperationRequest(statementId))
    //TODO
    ???
  }

  @throws[Exception]
  def updateCredentials(serializedCreds: Array[Byte]): JobHandle[Boolean] = {
    info(s"RSC client is updating credentials. length = ${serializedCreds.length}")
    //rscClient.submit(RemoteDriver.createUpdateTokensRequest(serializedCreds))
    //TODO
    ???
  }

  def stop(shutdownContext: Boolean): Unit = {
    rscClient.stop(shutdownContext)
  }

  def isClosed: Boolean = !rscClient.isAlive
}

object RpcClient {
  val SHARED_SINGLE_SESSION = "shared-single-session"
}
