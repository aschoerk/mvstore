import nioobjects.*
import pg.BLCKSZ
import java.nio.ByteBuffer

/**
 * @author aschoerk
 */

class PageXLogRecPtr(val xlogid: Int, val xrecoff: Int)

class PageHeaderData(b: NioBufferWithOffset) {
    val lsn = PageXLogRecPtr(b.getInt(0), b.getInt(4))
    val checksum = b.getShort(8)
    val flags = b.getShort(10)
    val lower = b.getShort(12)
    val upper = b.getShort(14)
    val special = b.getShort(16)
    val pageSizeVersion = b.getShort(18)
    val pruneXid = b.getInt(20)
}


class Page(b: ByteBuffer, number: Int) : NioBufferWithOffset(b, number * BLCKSZ) {
    val header = PageHeaderData(this)
    val numItems = (header.upper - header.lower) / 4
    fun getItemIdData(i: Int): ItemIdData {
        if (i >= numItems)
            throw IndexOutOfBoundsException("Accessing Page Items")
        return ItemIdData(getInt(24 + i * 4))
    }
    fun getItem(i: Int) = NioItem(b, getItemIdData(i))
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