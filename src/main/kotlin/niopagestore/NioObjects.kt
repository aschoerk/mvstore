/**
 * @author aschoerk
 */
package nioobjects

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.IntBuffer
import java.nio.channels.FileChannel

class TXIdentifier {

}


open class NioBufferWithOffset(val b: ByteBuffer, val offset: Int) {
    constructor(sizedBuffer: NioSizedObject)
            : this(sizedBuffer.orgBuffer.b, sizedBuffer.orgBuffer.offset + 8)
    constructor(b: NioBufferWithOffset)
            : this(b.b, b.offset)
    fun getByte(idx: Int) = b.get(offset + idx)
    fun setByte(idx: Int, i: Byte) {
        b.put(offset + idx, i)
    }
    fun getChar(idx: Int) = b.getChar(offset + idx)
    fun setChar(idx: Int, c: Char) {
        b.putChar(offset + idx, c)
    }

    fun getShort(idx: Int) = b.getShort(offset + idx)
    fun setShort(idx: Int, i: Short) {
        b.putShort(offset + idx, i)
    }

    fun getInt(idx: Int) = b.getInt(offset + idx)
    fun setInt(idx: Int, i: Int) {
        b.putInt(offset + idx, i)
    }

    fun getLong(idx: Int) = b.getLong(offset + idx)
    fun setLong(idx: Int, i: Long) {
        b.putLong(offset + idx, i)
    }

    fun getBoolean(idx: Int): Boolean = getByte(idx) != 0.toByte()
    fun setBoolean(idx: Int, b: Boolean) = setByte(idx, (if (b) 1 else 0).toByte())

}



open class NioSizedObject(val orgBuffer: NioBufferWithOffset, val expectedLength: Int) {
    constructor(orgBuffer: NioBufferWithOffset) : this(orgBuffer, -1)
    val len = orgBuffer.getLong(0)
    init {
        assert( expectedLength == -1 || expectedLength.toLong() == len )
    }
    fun bufferPartBehind()  = NioBufferWithOffset(orgBuffer.b, orgBuffer.offset + 8 + len.toInt())
}

public fun createMappedByteBuffer(filename: String): ByteBuffer {
    val f = RandomAccessFile(filename, "rw");
    val channel = f.channel

    val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, f.length());

    buffer.order(ByteOrder.LITTLE_ENDIAN)
    return buffer
}