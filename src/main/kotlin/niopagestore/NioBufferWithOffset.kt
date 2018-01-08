package niopagestore

import org.agrona.concurrent.MappedResizeableBuffer

open class NioBufferWithOffset(val b: MappedResizeableBuffer, val offset: Long) {
    constructor(b: NioBufferWithOffset)
            : this(b.b, b.offset)

    fun getByte(idx: Long) = b.getByte(offset + idx)
    fun setByte(idx: Long, i: Byte) {
        b.putByte(offset + idx, i)
    }

    fun getChar(idx: Long) = b.getChar(offset + idx)
    fun setChar(idx: Long, c: Char) {
        b.putChar(offset + idx, c)
    }

    fun getShort(idx: Long) = b.getShort(offset + idx)
    fun setShort(idx: Long, i: Short) {
        b.putShort(offset + idx, i)
    }

    fun getInt(idx: Long) = b.getInt(offset + idx)
    fun setInt(idx: Long, i: Int) {
        b.putInt(offset + idx, i)
    }

    fun getLong(idx: Long) = b.getLong(offset + idx)
    fun setLong(idx: Long, i: Long) {
        b.putLong(offset + idx, i)
    }

    fun getFloat(idx: Long) = b.getFloat(offset + idx)
    fun setFloat(idx: Long, f: Float) {
        b.putFloat(offset + idx, f)
    }

    fun getDouble(idx: Long) = b.getDouble(offset + idx)
    fun setDouble(idx: Long, f: Double) {
        b.putDouble(offset + idx, f)
    }

    fun getBoolean(idx: Long): Boolean = getByte(idx) != 0.toByte()
    fun setBoolean(idx: Long, b: Boolean) = setByte(idx, (if (b) 1 else 0).toByte())

    fun getByteArray(idx: Long, ba: ByteArray) = b.getBytes(idx, ba)
    fun setByteArray(idx: Long, ba: ByteArray) = b.putBytes(idx, ba)

    fun move(from: Long, to: Long, size: Int) {
        val buffer = ByteArray(size)
        b.getBytes(from, buffer)
        b.putBytes(to, buffer)
    }

}