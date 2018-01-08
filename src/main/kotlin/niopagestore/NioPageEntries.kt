package niopageentries

import niopageobjects.NioPageFile
import niopageentries.NioPageEntryType.*
import niopageobjects.NioPageFilePage
import niopageobjects.NioPageIndexEntry
import niopageobjects.PAGESIZE
import java.util.*

enum class NioPageEntryType {
    DUMMY, BYTE_ARRAY, PAGEENTRY_LIST, CHAR, BYTE,
    SHORT, INT, LONG, FLOAT, DOUBLE, STRING, ARRAY, BOOLEAN, EMPTY, ELSE }


interface NioPageEntry  {
    val length: Short
    val type: NioPageEntryType
    fun marshalTo(file: NioPageFile, offset: Long)
}

interface ComparableNioPageEntry : NioPageEntry, Comparable<ComparableNioPageEntry> {

}

abstract class NioPageNumberEntryBase(val number: Number) {
    fun compareTo(other: ComparableNioPageEntry): Int {
        if (this == other) return 0
        if (other !is NioPageNumberEntryBase) {
            if (other is EmptyPageEntry)
                return 1
            return hashCode().compareTo(other.hashCode())
        }
        else
            return (this.number.toDouble().compareTo((other as NioPageNumberEntryBase).number.toDouble()))
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is NioPageNumberEntryBase) return false

        if (number.toDouble() != other.number.toDouble()) return false

        return true
    }

    override fun hashCode(): Int {
        return number.toInt().hashCode()
    }

    override fun toString(): String {
        return "${this.javaClass.simpleName}(number=$number)"
    }

}

class ListPageEntry(c: Collection<NioPageEntry>) : ComparableNioPageEntry {
    constructor(a: Array<NioPageEntry>) : this(a.toList())
    val a = c.toMutableList()
    override fun compareTo(other: ComparableNioPageEntry): Int {
        if (this == other) return 0
        if (javaClass != other.javaClass) {
            if (other is EmptyPageEntry)
                return 1
            return hashCode().compareTo(other.hashCode())
        }
        other as ListPageEntry
        for (i in 1..minOf(this.a.size, other.a.size)) {
            val thisNioPageEntry = a[i - 1]
            val otherNioPageEntry = other.a[i - 1]
            if (thisNioPageEntry is ComparableNioPageEntry && otherNioPageEntry is ComparableNioPageEntry) {
                val tmp = thisNioPageEntry.compareTo(otherNioPageEntry)
                if (tmp != 0)
                    return tmp
            }
        }
        return this.a.size.compareTo(other.a.size)
    }

    override val length: Short
        get() = toShort(1 + 2 + a.sumBy { e -> e.length.toInt() })
    override val type: NioPageEntryType
        get() = PAGEENTRY_LIST //To change initializer of created properties use File | Settings | File Templates.

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, PAGEENTRY_LIST.ordinal.toByte())
        file.setShort(offset+1, toShort(length-3))
        var currentOffset = offset + 3
        for (e in a) {
            e.marshalTo(file, currentOffset)
            currentOffset += e.length
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ListPageEntry

        if (a != other.a) return false

        return true
    }

    override fun hashCode(): Int {
        return a.hashCode()
    }

    fun contains(entry: NioPageEntry) = a.contains(entry)
    override fun toString(): String {
        return "ListPageEntry(a=$a)"
    }

}

class BooleanPageEntry(val v: Boolean) : ComparableNioPageEntry, NioPageNumberEntryBase(if (v) 1 else 0) {
    override val length: Short
        get() = 2
    override val type: NioPageEntryType
        get() = BOOLEAN

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, BOOLEAN.ordinal.toByte())
        file.setByte(offset+1, if (v) 1 else 0)
    }
}

class EmptyPageEntry : ComparableNioPageEntry {
    override val length: Short
        get() = 1
    override val type: NioPageEntryType
        get() = EMPTY

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, EMPTY.ordinal.toByte())
    }

    override fun compareTo(other: ComparableNioPageEntry): Int {
        if (other !is EmptyPageEntry) return -1
        return 0
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EmptyPageEntry) return false
        return true
    }

    override fun hashCode(): Int {
        return Int.MIN_VALUE
    }

    override fun toString(): String {
        return "EmptyPageEntry()"
    }


}

class BytePageEntry(val v: Byte) : ComparableNioPageEntry, NioPageNumberEntryBase(v) {
    override val length: Short
        get() = 2
    override val type: NioPageEntryType
        get() = BYTE

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, BYTE.ordinal.toByte())
        file.setByte(offset+1, v)
    }
}

class CharPageEntry(val v: Char) : ComparableNioPageEntry, NioPageNumberEntryBase(v.toByte()) {
    constructor(s: Short) : this(s.toChar())
    override val length: Short
        get() = 3
    override val type: NioPageEntryType
        get() = CHAR

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, CHAR.ordinal.toByte())
        file.setShort(offset+1, v.toShort())
    }
}

class StringPageEntry(val s: String) : ComparableNioPageEntry {
    val b = s.toByteArray(Charsets.UTF_8)
    constructor(b: ByteArray) : this(String(b, Charsets.UTF_8))
    override val length: Short
        get() = toShort(b.size+3)
    override val type: NioPageEntryType
        get() = STRING

    override fun marshalTo(file: NioPageFile, offset: Long) {
        if (b.size > Short.MAX_VALUE)
            throw StringIndexOutOfBoundsException("Only Short sizes supported for strings in Pageentries")
        file.setByte(offset, STRING.ordinal.toByte())
        file.setShort(offset+1, toShort(b.size))
        file.setByteArray(offset+3, b)
    }

    override fun compareTo(other: ComparableNioPageEntry): Int {
        if (this == other) return 0
        if (javaClass != other.javaClass) {
            if (other is EmptyPageEntry)
                return 1
            return hashCode().compareTo(other.hashCode())
        }
        return this.s.compareTo((other as StringPageEntry).s)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StringPageEntry) return false

        if (s != other.s) return false

        return true
    }

    override fun hashCode(): Int {
        return s.hashCode()
    }

    override fun toString(): String {
        return "StringPageEntry(s='$s')"
    }


}

class ShortPageEntry(val v: Short) : ComparableNioPageEntry, NioPageNumberEntryBase(v) {
    override val length: Short
        get() = 3
    override val type: NioPageEntryType
        get() = SHORT

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, SHORT.ordinal.toByte())
        file.setShort(offset+1, v)
    }
}

class IntPageEntry(val v: Int) : ComparableNioPageEntry, NioPageNumberEntryBase(v) {
    override val length: Short
        get() = 5
    override val type: NioPageEntryType
        get() = INT

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, INT.ordinal.toByte())
        file.setInt(offset+1, v)
    }

}

class LongPageEntry(val v: Long) : ComparableNioPageEntry, NioPageNumberEntryBase(v) {
    override val length: Short
        get() = 9
    override val type: NioPageEntryType
        get() = LONG

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, LONG.ordinal.toByte())
        file.setLong(offset+1, v)
    }

}

class FloatPageEntry(val f: Float) : ComparableNioPageEntry, NioPageNumberEntryBase(f) {
    override val length: Short
        get() = 5
    override val type: NioPageEntryType
        get() = FLOAT

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, FLOAT.ordinal.toByte())
        file.setFloat(offset+1, f)
    }
}

class DoublePageEntry(val d: Double) : ComparableNioPageEntry, NioPageNumberEntryBase(d) {
    override val length: Short
        get() = 9
    override val type: NioPageEntryType
        get() = DOUBLE

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, DOUBLE.ordinal.toByte())
        file.setDouble(offset+1, d)
    }
}

inline  fun toShort(i: Int) : Short {
    if (i > Short.MAX_VALUE || i < Short.MIN_VALUE)
        throw IndexOutOfBoundsException("PageEntrylength does not fit in short")
    return i.toShort()
}

class ByteArrayPageEntry(val ba: ByteArray) : ComparableNioPageEntry {
    override val length: Short
        get() = toShort(ba.size + 3)
    override val type: NioPageEntryType
        get() = BYTE_ARRAY

    override fun marshalTo(file: NioPageFile, offset: Long) {
        file.setByte(offset, BYTE_ARRAY.ordinal.toByte())
        file.setShort(offset+1, toShort(ba.size))
        file.setByteArray(offset+3, ba)
    }

    override fun compareTo(other: ComparableNioPageEntry): Int {
        if (this === other) return 0
        if (javaClass != other.javaClass) {
            if (other is EmptyPageEntry)
                return 1
            return hashCode().compareTo(other.hashCode())
        }
        other as ByteArrayPageEntry
        return (1..minOf(this.ba.size, other.ba.size))
                .firstOrNull { ba[it -1] != other.ba[it -1] }
                ?: this.ba.size.compareTo(other.ba.size)

    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ByteArrayPageEntry) return false

        if (!Arrays.equals(ba, other.ba)) return false

        return true
    }

    override fun hashCode(): Int {
        return Arrays.hashCode(ba)
    }

    override fun toString(): String {
        return "ByteArrayPageEntry(ba=${Arrays.toString(ba)})"
    }


}

fun unmarshalFrom(file: NioPageFile, offset: NioPageIndexEntry): NioPageEntry {
    val pn = offset.entryOffset / PAGESIZE
    val page = NioPageFilePage(file, pn.toInt())
    return unmarshalFrom(file, page.offset(offset))
}

fun unmarshalFrom(file: NioPageFile, offset: Long): ComparableNioPageEntry {
    val type = values()[file.getByte(offset).toInt()]
    when (type) {
        BYTE -> return BytePageEntry(file.getByte(offset+1))
        CHAR -> return CharPageEntry(file.getShort(offset+1))
        SHORT -> return ShortPageEntry(file.getShort(offset+1))
        INT -> return IntPageEntry(file.getInt(offset+1))
        LONG -> return LongPageEntry(file.getLong(offset+1))
        FLOAT -> return FloatPageEntry(file.getFloat(offset+1))
        DOUBLE -> return DoublePageEntry(file.getDouble(offset+1))
        STRING -> {
            val len = file.getShort(offset+1)
            val result = ByteArray(len.toInt())
            file.getByteArray(offset+3,result)
            return StringPageEntry(result)
        }
        BYTE_ARRAY -> {
            val len = file.getShort(offset+1)
            val result = ByteArray(len.toInt())
            file.getByteArray(offset+3,result)
            return ByteArrayPageEntry(result)
        }
        PAGEENTRY_LIST -> {
            val len = file.getShort(offset+1)
            val a = mutableListOf<NioPageEntry>()
            var currentOffset = 0
            while(currentOffset < len) {
                val el = unmarshalFrom(file,offset + 3 + currentOffset)
                a.add(el)
                currentOffset += el.length
            }
            return ListPageEntry(a.toTypedArray())
        }
        EMPTY -> {
            return EmptyPageEntry()
        }
        else -> {
            throw IndexOutOfBoundsException("did not find valid NioPageEntry")
        }
    }
}