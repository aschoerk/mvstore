/**
 * @author aschoerk
 */

package niopageobjects

import niopagestore.INioBufferWithOffset
import niopagestore.MMapper
import niopagestore.NioBufferWithOffset
import niopagestore.NioPageFilePage
import org.agrona.concurrent.MappedResizeableBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

const val PAGESIZE = 8192L

const val PAGE_MAGIC = -0x12568762
// information in first page of file
const val ROOT_PAGE_OFFSET = 32L
const val FREESPACE_OFFSET = PAGESIZE

// structure and contents of freespace-page
const val START_OF_FREESPACE_MAP = 4L
const val FREEMAP_MAGIC = 0x16578954

// number of data-pages managed by one freespace-region
const val DATAPAGES_PER_FREESPACE_REGION = ((PAGESIZE - START_OF_FREESPACE_MAP) * 8).toInt()
const val PAGES_PER_FREESPACE_REGION = DATAPAGES_PER_FREESPACE_REGION + 1

class NioPageFile(val buffer: MappedResizeableBuffer, val length: Long) : NioBufferWithOffset(buffer, 0) {

    init {
        if (getInt(FREESPACE_OFFSET) != FREEMAP_MAGIC) {
            (FREESPACE_OFFSET..(length-1) step PAGES_PER_FREESPACE_REGION * PAGESIZE).forEach({
                assert(getInt(it) != FREEMAP_MAGIC)
            })
            (FREESPACE_OFFSET..(length-1) step PAGES_PER_FREESPACE_REGION * PAGESIZE).forEach({
                setInt(it, FREEMAP_MAGIC)  // init to magic to be able to identify already initialized file
                (it+4..it+PAGESIZE-1).forEach({setInt(it, 0)}) // make everything empty
            })
        } else {
            (FREESPACE_OFFSET..(length-1) step PAGES_PER_FREESPACE_REGION * PAGESIZE).forEach({
                assert(getInt(it) == FREEMAP_MAGIC)
            })
        }
    }

    var rootPageNumber
        get() = getInt(offset + ROOT_PAGE_OFFSET)
        set(value) = setInt(offset + ROOT_PAGE_OFFSET, value)


    inline fun pageOffset(page: Long) = PAGESIZE * page


    fun newPage(): NioPageFilePage {
        val pageNum = allocPage()
        if (pageNum == null)
            throw AssertionError("TODO: extend file")
        setInt(pageNum * PAGESIZE, 0)    // initialize page to show also by content, that it is new
        return NioPageFilePage(this, pageNum)
    }

    fun freePage(page: NioPageFilePage) = setFree(page.number)

    fun usedPagesIterator(): Iterator<NioPageFilePage> {

        return object : Iterator<NioPageFilePage> {
            var lastPageNumber = 0

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                if (lastPageNumber == 0) {
                    lastPageNumber = 1
                    advance()
                }
                return lastPageNumber * PAGESIZE < this@NioPageFile.length && this@NioPageFile.isUsed(lastPageNumber)
            }

            private fun advance() {
                var isUsed = false
                do {
                    lastPageNumber++
                    if ((lastPageNumber - 1) % PAGES_PER_FREESPACE_REGION == 0)
                        lastPageNumber++
                    if (lastPageNumber * PAGESIZE > this@NioPageFile.length)
                        return
                    isUsed = this@NioPageFile.isUsed(lastPageNumber)
                } while (!isUsed)
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): NioPageFilePage {
                if (hasNext()) {
                    val result = NioPageFilePage(this@NioPageFile, lastPageNumber)
                    advance()
                    return result
                }
                throw AssertionError("has not next")
            }

        }
    }


    var lastFreespaceOffset = FREESPACE_OFFSET
    var lastByteOffset = 4L

    private fun allocPage() : Int? {
        (lastFreespaceOffset..(length-1) step PAGES_PER_FREESPACE_REGION * PAGESIZE).forEach({
            freespaceOffset ->
                (lastByteOffset .. PAGESIZE-4).forEach({
                    byteOffset ->
                        val content = getInt(freespaceOffset  + byteOffset)
                        if (content != -1) {
                            var mask = 0x1
                            for (i in 0..31) {
                                if (content and mask == 0) {
                                    val pageNum = 1 +
                                            freespaceRegion(pageNumForOffset(freespaceOffset)) * PAGES_PER_FREESPACE_REGION +
                                            (byteOffset-4).toInt() * 8 + i + 1
                                    assert(isFree(pageNum))
                                    setInt(freespaceOffset  + byteOffset, content or mask)
                                    lastFreespaceOffset = freespaceOffset
                                    lastByteOffset = byteOffset
                                    assert(isUsed(pageNum))
                                    return pageNum
                                }
                                mask = mask shl 1
                            }
                            throw AssertionError("should not reach here")

                        }
                })
            lastByteOffset = 4
        })
        return null
    }

    private fun pageNumForOffset(freespaceOffset: Long): Int = (freespaceOffset / PAGESIZE).toInt()


    fun isUsed(pageNum: Int): Boolean {
        return getByte(freespaceRegionOffset(pageNum) + freespaceByteOffset(pageNum)).toInt() and freespaceByteMask(pageNum) != 0
    }

    fun isFree(pageNum: Int): Boolean {
        return getByte(freespaceRegionOffset(pageNum) + freespaceByteOffset(pageNum)).toInt() and freespaceByteMask(pageNum) == 0
    }


    private fun setUsed(pageNum: Int) {
        val tmp = getByte(freespaceRegionOffset(pageNum) + freespaceByteOffset(pageNum)).toInt()
        val mask = freespaceByteMask(pageNum)
        assert(tmp and mask == 0)
        setByte(freespaceRegionOffset(pageNum) + freespaceByteOffset(pageNum), (tmp or mask).toByte())
    }

    private fun setFree(pageNum: Int) {
        val tmp = getByte(freespaceRegionOffset(pageNum) + freespaceByteOffset(pageNum)).toInt()
        val mask = freespaceByteMask(pageNum)
        assert(tmp and mask != 0)
        setByte(freespaceRegionOffset(pageNum) + freespaceByteOffset(pageNum), (tmp xor mask).toByte())
    }
}

fun freespaceRegion(pageNum: Int) = (pageNum - 1) / PAGES_PER_FREESPACE_REGION

fun freespaceRegionOffset(pageNum: Int) = (freespaceRegion(pageNum) * PAGES_PER_FREESPACE_REGION + 1) * PAGESIZE

fun freespaceByteOffset(pageNum: Int) : Long {
    if (pageNum <= 0 || (pageNum - 1) % PAGES_PER_FREESPACE_REGION == 0)
        throw AssertionError("Page: $pageNum is no datapage")
    return ((pageNum * PAGESIZE) - freespaceRegionOffset(pageNum) - PAGESIZE) / (PAGESIZE * 8) + 4 // freespace-page itself
}

fun freespaceByteMask(pageNum: Int) : Int = 1 shl (((pageNum - 1) % PAGES_PER_FREESPACE_REGION - 1) % 8)


class NioTraFile(val file: NioPageFile) : INioBufferWithOffset {

    var startTime = java.lang.System.currentTimeMillis()
    val traCount = AtomicInteger(0)

    var threadLocal: ThreadLocal<TransactionInfo> = ThreadLocal()

    class TransactionInfo(val base: Int, val count: Int) {

        val pageMap = HashMap<Int,Int>()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as TransactionInfo

            if (base != other.base) return false
            if (count != other.count) return false

            return true
        }

        override fun hashCode(): Int {
            var result = base
            result = 31 * result + count
            return result
        }


    }

    fun begin() {
        if (threadLocal.get() != null) {
            throw IllegalStateException("Transaction running")
        }

        threadLocal.set(TransactionInfo((startTime and 0x4FFFFFF).toInt(), traCount.incrementAndGet()))
    }

    fun commit() {
        if (threadLocal.get() == null) {
            throw IllegalStateException("No Transaction running")
        }
        threadLocal.set(null)
    }

    fun copyPageIfNecessary(idx: Long) : Long {
        val pageNo = (idx / PAGESIZE).toInt()
        val transactionInfo = threadLocal.get()
        if (transactionInfo != null) {
            if (!transactionInfo.pageMap.containsKey(pageNo)) {
                val newPageNo = transactionInfo.pageMap[pageNo]
                if (newPageNo != null)
                    return newPageNo * PAGESIZE + idx - pageNo * PAGESIZE
                else
                    throw AssertionError("expected")
            } else {
                val newPage = file.newPage()
                file.move(pageNo * PAGESIZE, newPage.offset, PAGESIZE.toInt())
                transactionInfo.pageMap[pageNo] = newPage.number
                return newPage.number * PAGESIZE + idx - pageNo * PAGESIZE
            }
        } else {
            return idx
        }
    }

    override fun getByte(idx: Long): Byte {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setByte(idx: Long, i: Byte) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getChar(idx: Long): Char {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setChar(idx: Long, c: Char) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getShort(idx: Long): Short {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setShort(idx: Long, i: Short) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getInt(idx: Long): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setInt(idx: Long, i: Int) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLong(idx: Long): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setLong(idx: Long, i: Long) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getFloat(idx: Long): Float {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setFloat(idx: Long, f: Float) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getDouble(idx: Long): Double {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setDouble(idx: Long, f: Double) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getBoolean(idx: Long): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setBoolean(idx: Long, b: Boolean) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getByteArray(idx: Long, ba: ByteArray) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setByteArray(idx: Long, ba: ByteArray) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun move(from: Long, to: Long, size: Int) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}



fun main(args: Array<String>) {
    val m = MMapper("/tmp/test.bin", 100000L)
    m.putInt(1L, 100)
    assert(m.getInt(1L) == 100)
}