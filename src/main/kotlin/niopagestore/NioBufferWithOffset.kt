package niopagestore

import org.agrona.concurrent.MappedResizeableBuffer

interface INioBufferWithOffset {
    fun getByte(idx: Long): Byte
    fun setByte(idx: Long, i: Byte)
    fun getChar(idx: Long): Char
    fun setChar(idx: Long, c: Char)
    fun getShort(idx: Long): Short
    fun setShort(idx: Long, i: Short)
    fun getInt(idx: Long): Int
    fun setInt(idx: Long, i: Int)
    fun getLong(idx: Long): Long
    fun setLong(idx: Long, i: Long)
    fun getFloat(idx: Long): Float
    fun setFloat(idx: Long, f: Float)
    fun getDouble(idx: Long): Double
    fun setDouble(idx: Long, f: Double)
    fun getBoolean(idx: Long): Boolean
    fun setBoolean(idx: Long, b: Boolean)
    fun getByteArray(idx: Long, ba: ByteArray)
    fun setByteArray(idx: Long, ba: ByteArray)
    fun move(from: Long, to: Long, size: Int)
}

open class NioBufferWithOffset(val b: MappedResizeableBuffer, val offset: Long) : INioBufferWithOffset {
    constructor(b: NioBufferWithOffset)
            : this(b.b, b.offset)

    override fun getByte(idx: Long) = b.getByte(offset + idx)
    override fun setByte(idx: Long, i: Byte) {
        b.putByte(offset + idx, i)
    }

    override fun getChar(idx: Long) = b.getChar(offset + idx)
    override fun setChar(idx: Long, c: Char) {
        b.putChar(offset + idx, c)
    }

    override fun getShort(idx: Long) = b.getShort(offset + idx)
    override fun setShort(idx: Long, i: Short) {
        b.putShort(offset + idx, i)
    }

    override fun getInt(idx: Long) = b.getInt(offset + idx)
    override fun setInt(idx: Long, i: Int) {
        b.putInt(offset + idx, i)
    }

    override fun getLong(idx: Long) = b.getLong(offset + idx)
    override fun setLong(idx: Long, i: Long) {
        b.putLong(offset + idx, i)
    }

    override fun getFloat(idx: Long) = b.getFloat(offset + idx)
    override fun setFloat(idx: Long, f: Float) {
        b.putFloat(offset + idx, f)
    }

    override fun getDouble(idx: Long) = b.getDouble(offset + idx)
    override fun setDouble(idx: Long, f: Double) {
        b.putDouble(offset + idx, f)
    }

    override fun getBoolean(idx: Long): Boolean = getByte(idx) != 0.toByte()
    override fun setBoolean(idx: Long, b: Boolean) = setByte(idx, (if (b) 1 else 0).toByte())

    override fun getByteArray(idx: Long, ba: ByteArray) = b.getBytes(idx, ba)
    override fun setByteArray(idx: Long, ba: ByteArray) = b.putBytes(idx, ba)

    override fun move(from: Long, to: Long, size: Int) {
        val buffer = ByteArray(size)
        b.getBytes(from, buffer)
        b.putBytes(to, buffer)
    }

}