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

package org.apache.livy.thriftserver.rpc

import java.io._

import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.io.Writable
import org.apache.thrift.TByteArrayOutputStream

/**
 * Util methods for (de-) serialization
 */
object RpcUtil {
  // TByteArrayOutputStream ensures we don't need to copy the internal buffer
  // Must ensure it is reset after each get()
  private val serBufferThLocal = new ThreadLocal[TByteArrayOutputStream]() {
    override protected def initialValue = new TByteArrayOutputStream(1024)
  }
  // try up to 2 times to drain remaining pending bytes. Ideally there should be
  // no bytes pending - but there might be some footer, etc remaining.
  private val NUM_DRAIN_RETRY = 2

  // Ensure we read only up to specified bytes from the input stream.
  private class KryoInputAdapter(var input: Input, var remaining: Int) extends InputStream {
    @throws[IOException]
    override def read: Int = {
      if (remaining <= 0) return -1
      val retval = input.read
      remaining -= 1
      retval
    }

    @throws[IOException]
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (remaining <= 0) return -1
      val size = Math.min(len, remaining)
      val actualRead = input.read(b, off, size)
      remaining -= actualRead
      actualRead
    }

    @throws[IOException]
    override def skip(n: Long): Long = {
      val toSkip = Math.min(n, remaining)
      val actualSkip = input.skip(toSkip)
      remaining -= actualSkip.toInt
      actualSkip
    }

    @throws[IOException]
    override def available: Int = remaining

    @throws[IOException]
    override def close(): Unit = remaining = 0

    override def mark(readlimit: Int): Unit =
      throw new UnsupportedOperationException("Mark/reset not supported")

    @throws[IOException]
    override def reset(): Unit = throw new IOException("Mark/reset not supported")

    override def markSupported: Boolean = false

    @throws[IOException]
    private[rpc] def drain() = {
      var retry = 0
      while (remaining > 0) {
        val skipped = skip(remaining)
        if (skipped <= 0) {
          if (retry > NUM_DRAIN_RETRY) {
            throw new IOException(s"Unable to drain remaining = $remaining bytes")
          }
          retry += 1
        }
        else retry = 0
      }
    }
  }

  def serialize(obj: Serializable, output: Output): Unit = {
    val baos = serBufferThLocal.get
    try {
      baos.reset()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      oos.flush()
      oos.close()
      val length = baos.size
      output.writeInt(length, true)
      output.write(baos.get, 0, length)
    } catch {
      case ioEx: IOException =>
        throw new IllegalStateException(s"Unable to serialize $obj", ioEx)
    }
  }

  def deserialize(input: Input): Serializable = {
    val length = input.readInt(true)
    try {
      val stream = new RpcUtil.KryoInputAdapter(input, length)
      val ois = new ObjectInputStream(stream)
      val retval = ois.readObject.asInstanceOf[Serializable]
      // Ensure we drain the stream and don't leave any bytes around
      stream.drain()
      retval
    } catch {
      case e@(_: ClassNotFoundException | _: IOException) =>
        throw new IllegalStateException(s"Unable to deserialize trowSet from $length bytes", e)
    }
  }

  def serializeToBytes(obj: Writable): Array[Byte] = {
    val baos = serBufferThLocal.get
    try {
      baos.reset()
      val dataOutput = new DataOutputStream(baos)
      obj.write(dataOutput)
      dataOutput.flush()
      dataOutput.close()
      baos.toByteArray
    } catch {
      case ioEx: IOException =>
        throw new IllegalStateException(s"Unable to serialize $obj", ioEx)
    }
  }

  def serializeToBytes(obj: Serializable): Array[Byte] = {
    val baos = serBufferThLocal.get
    try {
      baos.reset()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      oos.flush()
      oos.close()
      baos.toByteArray
    } catch {
      case ioEx: IOException =>
        throw new IllegalStateException(s"Unable to serialize $obj", ioEx)
    }
  }

  def deserializeFromBytes(bytes: Array[Byte]): Serializable = try {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[Serializable]
  } catch {
    case ex@(_: IOException | _: ClassNotFoundException) =>
      throw new IllegalStateException("Unable to deserialize from input", ex)
  }

  def deserializeFromBytes(writable: Writable, bytes: Array[Byte]): Unit = try {
    val bis = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bis)
    writable.readFields(dis)
  } catch {
    case ex: IOException =>
      throw new IllegalStateException(s"Unable to deserialize from input for $writable", ex)
  }
}
