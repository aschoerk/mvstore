import nioobjects.*
import pg.BLCKSZ
import java.nio.ByteBuffer

/**
 * @author aschoerk
 */

class PageXLogRecPtr(val xlogid: Int, val xrecoff: Int)

class PageHeaderData(b: NioBufferWithOffset) : NioBufferWithOffset(b){
    val lsn get() = PageXLogRecPtr(getInt(0), getInt(4))
    val checksum get() =  getShort(8)
    val flags get() =  getShort(10)
    val lower get() =  getShort(12)
    val upper get() =  getShort(14)
    val special get() =  getShort(16)
    val pageSizeVersion get() =  getShort(18)
    val pruneXid get() =  getInt(20)
    fun getItemIdData(i: Int) : ItemIdData {
        val numItems = if (lower <= 24) 0 else (upper - lower) / 4
        if (i >= numItems)
            throw IndexOutOfBoundsException("Accessing Page Items")
        return ItemIdData(getInt(24 + i * 4))
    }

}

class ItemIdData(data: Int) {
    val off = data and 0x7FFF
    val len = data ushr 17
    val flag1 = data shr 15 and 1
    val flag2 = data shr 16 and 1
}


class NioItem(b: NioBufferWithOffset, val itemIdData: ItemIdData) : NioBufferWithOffset(b.b, b.offset + itemIdData.off) {
    override fun toString() : String {
        val sb = StringBuilder()
        val cb = StringBuilder()
        for(i in 0..itemIdData.len-1) {
            val b = getByte(i)
            if (sb.length > 0) sb.append(" ")
            if (b in 31..126) {
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

class Page(b: ByteBuffer, number: Int) : NioBufferWithOffset(b, number * BLCKSZ) {
    val header = PageHeaderData(this)
    val numItems = (header.upper - header.lower) / 4
    fun getItemIdData(i: Int): ItemIdData {
        if (i >= numItems)
            throw IndexOutOfBoundsException("Accessing Page Items")
        return ItemIdData(getInt(24 + i * 4))
    }
    fun getItem(i: Int) = NioItem(this, getItemIdData(i))
}

fun main(args: Array<String>) {

    val pg_attribute_relid_attnam_index = createMappedByteBuffer("/home/aschoerk/projects/mvcc/17178/2658")

    val pg_attribute = createMappedByteBuffer("/home/aschoerk/projects/mvcc/17178/1249")

    println("pg_attribute")
    outputPages(pg_attribute)

    println("\n\npg_attribute_relid_attnam_index")
    outputPages(pg_attribute_relid_attnam_index)

}

private fun outputPages(pg_attribute: ByteBuffer) {
    for (i in 0..minOf(20, pg_attribute.limit() / BLCKSZ - 1)) {
        val p = Page(pg_attribute, i)
        println("Page: " + i + " items: " + p.numItems)
        for (j in 0..p.numItems-1) {
            val item = p.getItem(j)
            println("Item: " + j + ": " + item)
        }
    }
}