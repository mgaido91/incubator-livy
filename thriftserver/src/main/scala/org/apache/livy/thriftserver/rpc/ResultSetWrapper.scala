package org.apache.livy.thriftserver.rpc

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hive.service.cli.{ColumnBasedSet, RowBasedSet, RowSet, TableSchema}
import org.apache.hive.service.rpc.thrift.TRowSet

object ResultSetWrapper {
  // For now, hardcoded in code to test. (note, using env, etc might be
  // risky if we cant ensure it is consistently set in a distributed setting).
  // Ideally, we want to use ColumnBasedSet - and fallback to RowBasedSet
  // only if we are unable to make ColumnBasedSet work.
  val enableColumnSet = true

  @SuppressWarnings(Array("ConstantConditions"))
  def create(schema: TableSchema): ResultSetWrapper = {
    if (enableColumnSet) new ResultSetWrapper.ColumnBasedResultSetWrapper(schema)
    else new ResultSetWrapper.RowBasedResultSetWrapper(schema)
  }

  private class ColumnBasedResultSetWrapper private[rpc](val schema: TableSchema)
      extends ResultSetWrapper {
    private var delegate = new ColumnBasedSet(schema)

    override def toRowSet: RowSet = delegate

    override def addRow(data: Array[AnyRef]): Unit = {
      delegate.addRow(data)
    }

    override protected def getAsTRowSet: TRowSet = delegate.toTRowSet

    override protected def deserializeFromTRowSet(set: TRowSet): Unit = {
      this.delegate = new ColumnBasedSet(set)
    }
  }

  // RowBasedSet seems to be handled fine by kryo
  private class RowBasedResultSetWrapper private[rpc](val schema: TableSchema) extends ResultSetWrapper {
    private var delegate = new RowBasedSet(schema)

    override def toRowSet: RowSet = delegate

    override def addRow(data: Array[AnyRef]): Unit = {
      delegate.addRow(data)
    }

    override protected def getAsTRowSet: TRowSet = delegate.toTRowSet

    override protected def deserializeFromTRowSet(set: TRowSet): Unit = {
      this.delegate = new RowBasedSet(set)
    }
  }

}

abstract class ResultSetWrapper extends KryoSerializable {
  def toRowSet: RowSet

  def addRow(data: Array[AnyRef]): Unit

  protected def getAsTRowSet: TRowSet

  protected def deserializeFromTRowSet(set: TRowSet): Unit

  // Simple implementation to work around kryo issue - perf not (yet) a concern
  override def write(kryo: Kryo, output: Output): Unit = {
    val trowSet = getAsTRowSet
    RpcUtil.serialize(trowSet, output)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    deserializeFromTRowSet(RpcUtil.deserialize(input).asInstanceOf[TRowSet])
  }
}
