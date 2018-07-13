package org.apache.livy.thriftserver.rpc

import org.apache.livy._
import org.apache.hive.service.cli.{SessionHandle, TableSchema}
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.thriftserver.LivyThriftServer
import org.apache.livy.thriftserver.utils.HiveTypes
import org.apache.livy.utils.LivySparkUtils

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Try


class RpcClient(livySession: InteractiveSession) extends Logging {
  import RpcClient._

  private val (sparkMajorVersion, _) =
    LivySparkUtils.formatSparkVersion(livySession.livyConf.get(LivyConf.LIVY_SPARK_VERSION))
  private val defaultIncrementalCollect =
    livySession.livyConf.getBoolean(LivyConf.THRIFT_INCR_COLLECT_ENABLED).toString

  private val rscClient = livySession.client.get

  private def getSparkEntry(jobContext: JobContext): AnyRef = {
    if (sparkMajorVersion == 1) {
      Option(jobContext.hivectx()).getOrElse(jobContext.sqlctx())
    } else {
      jobContext.sparkSession()
    }
  }

  def sessionId(sessionHandle: SessionHandle): String = {
    sessionHandle.getSessionId.toString
  }

  private def getSessionSparkEntry(jobContext: JobContext, sessionId: String): AnyRef = {
    jobContext.getSharedObject[HashMap[String, AnyRef]](SPARK_CONTEXT_MAP)(sessionId)
  }

  @throws[Exception]
  def executeSql(
      sessionHandle: SessionHandle,
      statementId: String,
      statement: String): JobHandle[_] = {
    val parentSessionId = sessionId(sessionHandle)
    info(s"RSC client is executing SQL query: $statement, statementId = $statementId, session = " +
      sessionHandle)
    if (null == statementId || null == statement) {
      throw new IllegalArgumentException("Invalid statement/statementId specified. " +
        s"Statement = $statement, statementId = $statementId")
    }
    rscClient.submit(new Job[Boolean] {
      override def call(jc: JobContext): Boolean = {
        debug(s"SqlStatementRequest. statementId = $statementId, statement = $statement")
        val sparkContext = jc.sc()
        sparkContext.synchronized {
          sparkContext.setJobGroup(statementId, statement)
        }
        val spark = getSessionSparkEntry(jc, parentSessionId)
        val result = spark.getClass.getMethod("sql", classOf[String]).invoke(spark, statement)
        val schema = result.getClass.getMethod("schema").invoke(result)
        val jsonString = schema.getClass.getMethod("json").invoke(schema).asInstanceOf[String]
        val tableSchema = HiveTypes.tableSchemaFromSparkJson(jsonString)

        // Set the schema in the shared map
        sparkContext.synchronized {
          val existingMap = jc.getSharedObject[HashMap[String, TableSchema]](STATEMENT_SCHEMA_MAP)
          jc.setSharedObject(STATEMENT_SCHEMA_MAP, existingMap + (statementId -> tableSchema))
        }

        val incrementalCollect = {
          if (sparkMajorVersion == 1) {
            spark.getClass.getMethod("getConf", classOf[String], classOf[String])
              .invoke(spark,
                LivyThriftServer.INCR_COLLECT_ENABLED_WITHPREFIX,
                defaultIncrementalCollect)
              .asInstanceOf[String].toBoolean
          } else {
            val conf = spark.getClass.getMethod("conf").invoke(spark)
            conf.getClass.getMethod("get", classOf[String], classOf[String])
              .invoke(conf,
                LivyThriftServer.INCR_COLLECT_ENABLED_WITHPREFIX,
                defaultIncrementalCollect)
              .asInstanceOf[String].toBoolean
          }
        }

        val iter = if (incrementalCollect) {
          val rdd = result.getClass.getMethod("rdd").invoke(result)
          rdd.getClass.getMethod("toLocalIterator").invoke(rdd).asInstanceOf[Iterator[_]]
        } else {
          result.getClass.getMethod("collect").invoke(result).asInstanceOf[Array[_]].iterator
        }

        // Set the iterator in the shared map
        sparkContext.synchronized {
          val existingMap =
            jc.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP)
          jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, existingMap + (statementId -> iter))
        }

        true
      }
    })
  }

  @throws[Exception]
  def fetchResult(statementId: String, maxRows: Int): JobHandle[ResultSetWrapper] = {
    info(s"RSC client is fetching result for statementId $statementId with $maxRows maxRows.")
    if (null == statementId) {
      throw new IllegalArgumentException(
        s"Invalid statementId specified. StatementId = $statementId")
    }
    rscClient.submit(new Job[ResultSetWrapper] {
      override def call(jobContext: JobContext): ResultSetWrapper = {
        val statementIterMap =
          jobContext.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP)
        val iter = statementIterMap(statementId)

        if (null == iter) {
          // Previous query execution failed.
          throw new NoSuchElementException("No successful query executed for output")
        }
        val statementSchemaMap =
          jobContext.getSharedObject[HashMap[String, TableSchema]](STATEMENT_SCHEMA_MAP)
        val tableSchema = statementSchemaMap(statementId)

        val resultRowSet = ResultSetWrapper.create(tableSchema)
        val numOfColumns = tableSchema.getSize
        if (!iter.hasNext) {
          resultRowSet
        } else {
          var curRow = 0
          while (curRow < maxRows && iter.hasNext) {
            val sparkRow = iter.next()
            val row = ArrayBuffer[Any]()
            var curCol: Integer = 0
            while (curCol < numOfColumns) {
              row += sparkRow.getClass.getMethod("get", classOf[Int]).invoke(sparkRow, curCol)
              curCol += 1
            }
            resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
            curRow += 1
          }
          resultRowSet
        }
      }
    })
  }

  @throws[Exception]
  def fetchResultSchema(statementId: String): JobHandle[TableSchema] = {
    info(s"RSC client is fetching result schema for statementId = $statementId")
    if (null == statementId) {
      throw new IllegalArgumentException(
        s"Invalid statementId specified. statementId = $statementId")
    }
    rscClient.submit(new Job[TableSchema] {
      override def call(jobContext: JobContext): TableSchema = {
        jobContext.getSharedObject[HashMap[String, TableSchema]](STATEMENT_SCHEMA_MAP)(statementId)
      }
    })
  }

  @throws[Exception]
  def cleanupStatement(statementId: String, cancelJob: Boolean = false): JobHandle[_] = {
    info(s"Cleaning up remote session for statementId = $statementId")
    if (null == statementId) {
      throw new IllegalArgumentException(
        s"Invalid statementId specified. statementId = $statementId")
    }
    rscClient.submit(new Job[Boolean] {
      override def call(jc: JobContext): Boolean = {
        val sparkContext = jc.sc()
        sparkContext.cancelJobGroup(statementId)
        sparkContext.synchronized {
          // Clear job group only if current job group is same as expected job group.
          if (sparkContext.getLocalProperty("spark.jobGroup.id") == statementId) {
            sparkContext.clearJobGroup()
          }
          val iterMap = jc.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP)
          jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, iterMap - statementId)
          val schemaMap = jc.getSharedObject[HashMap[String, TableSchema]](STATEMENT_SCHEMA_MAP)
          jc.setSharedObject(STATEMENT_SCHEMA_MAP, schemaMap - statementId)
        }
        true
      }
    })
  }

  /**
   * Creates a new Spark context for the specified session and stores it in a shared variable so
   * that any incoming session uses a different one: it is needed in order to avoid interactions
   * between different users working on the same remote Livy session (eg. setting a property,
   * changing databsae, etc.).
   */
  @throws[Exception]
  def executeRegisterSession(sessionHandle: SessionHandle): JobHandle[_] = {
    val parentSessionId = sessionId(sessionHandle)
    info(s"RSC client is executing register session $sessionHandle")
    rscClient.submit(new Job[Boolean] {
      override def call(jc: JobContext): Boolean = {
        debug(s"RegisterSessionRequest. parentSessionId = $parentSessionId")
        val spark = getSparkEntry(jc)
        val sessionSpecificSpark = spark.getClass.getMethod("newInstance").invoke(spark)
        jc.sc().synchronized {
          val existingMap =
            Try(jc.getSharedObject[HashMap[String, AnyRef]](SPARK_CONTEXT_MAP))
              .getOrElse(new HashMap[String, AnyRef]())
          jc.setSharedObject(SPARK_CONTEXT_MAP,
            existingMap + (parentSessionId -> sessionSpecificSpark))
          Try(jc.getSharedObject[HashMap[String, TableSchema]](STATEMENT_SCHEMA_MAP))
            .failed.foreach { _ =>
              jc.setSharedObject(STATEMENT_SCHEMA_MAP, new HashMap[String, TableSchema]())
            }
          Try(jc.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP))
            .failed.foreach { _ =>
              jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, new HashMap[String, Iterator[_]]())
            }
        }
        true
      }
    })
  }

  /**
   * Removes the Spark session created for the specified session from the shared variable.
   */
  @throws[Exception]
  def executeUnregisterSession(sessionHandle: SessionHandle): JobHandle[_] = {
    val parentSessionId = sessionId(sessionHandle)
    info(s"RSC client is executing unregister session $sessionHandle")
    rscClient.submit(new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {
        debug(s"UnregisterSessionRequest. parentSessionId = $parentSessionId")
        jobContext.sc().synchronized {
          val existingMap =
            jobContext.getSharedObject[HashMap[String, AnyRef]](SPARK_CONTEXT_MAP)
          jobContext.setSharedObject(SPARK_CONTEXT_MAP, existingMap - parentSessionId)
        }
        true
      }
    })
  }
}

object RpcClient {
  val SPARK_CONTEXT_MAP = "sparkContextMap"
  val STATEMENT_RESULT_ITER_MAP = "statementIdToResultIter"
  val STATEMENT_SCHEMA_MAP = "statementIdToSchema"
}
