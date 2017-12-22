/**
 * @author aschoerk
 */
package nioobjects

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.IntBuffer
import java.nio.channels.FileChannel
import kotlin.test.assertTrue



class NioObjectBuffer(val b: IntBuffer, val baseIndex: Int) {
    var idx = baseIndex shr 2

    fun getLong(): Long {
        val lo = b[idx++]
        val hi = b[idx++]
        return hi.toLong() shl 32 or lo.toLong()
    }

    fun getInt(): Int {
        return b[idx++]
    }

    fun getInt(idx: Int): Int {
        this.idx = idx shr 2
        return getInt()
    }

    fun getBoolean(idx: Int): Boolean {
        this.idx = idx shr 2
        val value = getInt()
        return value shr (idx and 3) * 8 != 0
    }

    fun getByte(idx: Int): Byte {
        this.idx = idx shr 2
        val value = getInt()
        return (value shr (idx and 3) * 8 and 0xFF).toByte()
    }

    fun getShort(idx: Int): Short {
        this.idx = idx shr 2
        val value = getInt()
        return (value shr (idx and 3) * 8 and 0xFFFF).toShort()
    }

    fun getLong(idx: Int): Long {
        this.idx = idx shr 2
        val lo = getInt()
        val hi = getInt()
        return hi.toLong() shl 32 or lo.toLong()
    }

}

open class NioBufferWithOffset(val b: ByteBuffer, val offset: Int) {
    constructor(sizedBuffer: NioSizedObject)
            : this(sizedBuffer.orgBuffer.b, sizedBuffer.orgBuffer.offset + 8)
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

class ItemIdData(data: Int) {
    val off = data and 0x7FFF
    val len = data ushr 17
    val flag1 = data shr 15 and 1
    val flag2 = data shr 16 and 1
}


class NioItem(b: ByteBuffer, val itemIdData: ItemIdData) : NioBufferWithOffset(b, itemIdData.off) {
    override fun toString() : String {
        val sb = StringBuilder()
        val cb = StringBuilder()
        for(i in 0..itemIdData.len-1) {
            val b = getByte(i)
            if (sb.length > 0) sb.append(" ")
            if (b >= 31 && b <= 126) {
                cb.append(b.toChar())
            } else {
                cb.append('.')
            }
            val out = ((b.toInt()) and 0xFF).toString(16)
            if (out.length == 1)
                sb.append("0")
            sb.append(out)
        }
        sb.append("\n    - ")
        sb.append(cb)
        return sb.toString()
    }

}

open class NioSizedObject(val orgBuffer: NioBufferWithOffset, val expectedLength: Int) {
    constructor(orgBuffer: NioBufferWithOffset) : this(orgBuffer, -1)
    val len = orgBuffer.getLong(0)
    init {
        assertTrue { expectedLength == -1 || expectedLength.toLong() == len }
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