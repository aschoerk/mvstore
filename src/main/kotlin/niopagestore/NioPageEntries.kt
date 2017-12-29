package niopagestore

import niopageobjects.NioPageFile
import niopagestore.NioPageEntryType.*

enum class NioPageEntryType {
    BYTE_ARRAY, PAGEENTRY_ARRAY, CHAR, BYTE,
    SHORT, INT, LONG, FLOAT, DOUBLE, STRING, ARRAY, BOOLEAN }


interface NioPageEntry {
    val length: Int
    val type: NioPageEntryType
    fun marshalTo(file: NioPageFile, offset: Long)
}

class ArrayPageEntry(val a: Array<NioPageEntry>) : NioPageEntry {
    override val length: Int
        get() = 1 + 4 + a.sumBy { e -> e.length }//To change initializer of created properties use File | Settings | File Templates.
    override val type: NioPageEntryType
        get() = PAGEENTRY_ARRAY //To change initializer of created properties use File | Settings | File Templates.

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, BYTE_ARRAY.ordinal.toByte())
        file.setInt(offset+1, length-5)
        var currentOffset = offset + 5
        for (e in a) {
            e.marshalTo(file, currentOffset)
            currentOffset += e.length
        }
    }

}

class BooleanPageEntry(val v: Boolean) : NioPageEntry {
    override val length: Int
        get() = 2
    override val type: NioPageEntryType
        get() = BOOLEAN

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, BOOLEAN.ordinal.toByte())
        file.setByte(offset+1, if (v) 1 else 0)
    }
}

class BytePageEntry(val v: Byte) : NioPageEntry {
    override val length: Int
        get() = 2
    override val type: NioPageEntryType
        get() = BYTE

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, BYTE.ordinal.toByte())
        file.setByte(offset+1, v)
    }
}

class CharPageEntry(val v: Char) : NioPageEntry {
    override val length: Int
        get() = 2
    override val type: NioPageEntryType
        get() = CHAR

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, CHAR.ordinal.toByte())
        file.setChar(offset+1, v)
    }
}

class StringPageEntry(val b: ByteArray) : NioPageEntry {
    constructor(s: String) : this(s.toByteArray(Charsets.UTF_8))
    override val length: Int
        get() = b.size+5
    override val type: NioPageEntryType
        get() = STRING

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, STRING.ordinal.toByte())
        file.setInt(offset+1, b.size)
        file.setByteArray(offset+5, b)
    }
}

class ShortPageEntry(val v: Short) : NioPageEntry {
    override val length: Int
        get() = 2
    override val type: NioPageEntryType
        get() = SHORT

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, SHORT.ordinal.toByte())
        file.setShort(offset+1, v)
    }
}

class IntPageEntry(val v: Int) : NioPageEntry {
    override val length: Int
        get() = 5
    override val type: NioPageEntryType
        get() = INT

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, INT.ordinal.toByte())
        file.setInt(offset+1, v)
    }

}

class LongPageEntry(val v: Long) : NioPageEntry {
    override val length: Int
        get() = 9
    override val type: NioPageEntryType
        get() = LONG

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, LONG.ordinal.toByte())
        file.setLong(offset+1, v)
    }

}

class FloatPageEntry(val b: Float) : NioPageEntry {
    override val length: Int
        get() = 5
    override val type: NioPageEntryType
        get() = FLOAT

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, FLOAT.ordinal.toByte())
        file.setFloat(offset+1, b)
    }
}

class DoublePageEntry(val b: Double) : NioPageEntry {
    override val length: Int
        get() = 9
    override val type: NioPageEntryType
        get() = DOUBLE

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, DOUBLE.ordinal.toByte())
        file.setDouble(offset+1, b)
    }
}

class ByteArrayPageEntry(val ba: ByteArray) : NioPageEntry {
    override val length: Int
        get() = ba.size + 5
    override val type: NioPageEntryType
        get() = BYTE_ARRAY

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, BYTE_ARRAY.ordinal.toByte())
        file.setInt(offset, ba.size)
        file.setByteArray(offset+5, ba)
    }
}

fun marshalFrom(file: NioPageFile, offset: Long): NioPageEntry {
    val type = values()[file.getByte(offset).toInt()]
    when (type) {
        BYTE -> return BytePageEntry(file.getByte(offset+1))
        CHAR -> return CharPageEntry(file.getChar(offset+1))
        SHORT -> return ShortPageEntry(file.getShort(offset+1))
        INT -> return IntPageEntry(file.getInt(offset+1))
        LONG -> return LongPageEntry(file.getLong(offset+1))
        FLOAT -> return FloatPageEntry(file.getFloat(offset+1))
        DOUBLE -> return DoublePageEntry(file.getDouble(offset+1))
        STRING -> {
            val len = file.getInt(offset+1)
            val result = ByteArray(len)
            file.getByteArray(offset+5,result)
            return StringPageEntry(result)
        }
        BYTE_ARRAY -> {
            val len = file.getInt(offset+1)
            val result = ByteArray(len)
            file.getByteArray(offset+5,result)
            return ByteArrayPageEntry(result)
        }
        PAGEENTRY_ARRAY -> {
            val len = file.getInt(offset+1)
            val a: MutableList<NioPageEntry> = mutableListOf()
            var currentOffset = 0;
            while(currentOffset < len) {
                val el = marshalFrom(file,offset + 5 + currentOffset)
                a.add(el)
                currentOffset += el.length
            }
            return ArrayPageEntry(a.toTypedArray())
        }
        else -> {
            throw IndexOutOfBoundsException("did not find valud NioPageEntry")
        }
    }

}