/**
 * @author aschoerk
 */

package niopageobjects

import niopagestore.*
import org.agrona.concurrent.MappedResizeableBuffer
import java.util.concurrent.atomic.AtomicInteger

const val PAGESIZE = 8192L


// information in first page of file

const val FREESPACE_OFFSET = PAGESIZE

// structure and contents of freespace-page
const val START_OF_FREESPACE_MAP = 4L
const val FREEMAP_MAGIC = 0x16578954

// number of data-pages managed by one freespace-region
const val DATAPAGES_PER_FREESPACE_REGION = ((PAGESIZE - START_OF_FREESPACE_MAP) * 8).toInt()
const val PAGES_PER_FREESPACE_REGION = DATAPAGES_PER_FREESPACE_REGION + 1


interface INioPageFile : INioBufferWithOffset {
    fun newPage(): NioPageFilePage
    fun freePage(page: NioPageFilePage)
    fun isUsed(pageNum: Int): Boolean
    fun isFree(pageNum: Int): Boolean
}


open class NioPageFile(val buffer: MappedResizeableBuffer, val length: Long) : MMapBufferWithOffset(buffer, 0), INioBufferWithOffset, INioPageFile {
    fun checkFreeSpace() {
        (FREESPACE_OFFSET..(length-1) step PAGES_PER_FREESPACE_REGION * PAGESIZE).forEach({
            assert(getInt(it) == FREEMAP_MAGIC)
        })
    }

    fun initFreeSpaceSafely() {
        (FREESPACE_OFFSET..(length-1) step PAGES_PER_FREESPACE_REGION * PAGESIZE).forEach({
            assert(getInt(it) != FREEMAP_MAGIC)
        })
        (FREESPACE_OFFSET..(length-1) step PAGES_PER_FREESPACE_REGION * PAGESIZE).forEach({
            setInt(it, FREEMAP_MAGIC)  // init to magic to be able to identify already initialized file
            (it+4..it+PAGESIZE-1 step 4).forEach({setInt(it, 0)}) // make everything empty
        })
    }


    override fun newPage(): NioPageFilePage {
        val pageNum = allocPage()
        if (pageNum == null)
            throw AssertionError("TODO: extend file")
        setInt(pageNum * PAGESIZE, 0)    // initialize page to show also by content, that it is new
        return NioPageFilePage(this, pageNum)
    }

    override fun freePage(page: NioPageFilePage) = setFree(page.number)

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


    private var lastFreespaceOffset = FREESPACE_OFFSET
    private var lastByteOffset = 4L

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


    override fun isUsed(pageNum: Int): Boolean {
        return getByte(freespaceRegionOffset(pageNum) + freespaceByteOffset(pageNum)).toInt() and freespaceByteMask(pageNum) != 0
    }

    override fun isFree(pageNum: Int): Boolean {
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
        throw AssertionError("pg.Page: $pageNum is no datapage")
    return ((pageNum * PAGESIZE) - freespaceRegionOffset(pageNum) - PAGESIZE) / (PAGESIZE * 8) + 4 // freespace-page itself
}

fun freespaceByteMask(pageNum: Int) : Int = 1 shl (((pageNum - 1) % PAGES_PER_FREESPACE_REGION - 1) % 8)

object MVCC {
    var startTime = java.lang.System.currentTimeMillis()
    val traCount = AtomicInteger(0)
    var threadLocal: ThreadLocal<TransactionInfo> = ThreadLocal()
    val transactions = mutableMapOf<Long,TransactionInfo>()

    class TransactionInfo(val base: Int, val count: Int) {

        val undoMap = HashMap<Int,Int>()
        val freedPages = HashSet<Int>()

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

        val traNum: Long
            get() = base.toLong() shl 32 or count.toLong()

    }

    fun begin() {
        if (threadLocal.get() != null) {
            throw IllegalStateException("Transaction running")
        }

        val transactionInfo = TransactionInfo(((startTime / 1000) and 0x4FFFFFFF).toInt(), traCount.incrementAndGet())
        threadLocal.set(transactionInfo)
        transactions[transactionInfo.traNum] = transactionInfo
    }

    fun commit() {
        if (threadLocal.get() == null) {
            throw IllegalStateException("No Transaction running")
        }
        threadLocal.set(null)
    }

    fun current() = threadLocal.get()

    operator fun get(traId: Long) = transactions[traId]

}



class MVCCFile(val file: NioPageFile) : INioPageFile {

    val b = file


    fun copyPageIfNecessary(idx: Long) {
        val pageNo = (idx / PAGESIZE).toInt()
        val transactionInfo = MVCC.current()
        if (transactionInfo != null) {
            if (!transactionInfo.undoMap.containsKey(pageNo)) {
                val newPage = file.newPage()
                file.move(pageNo * PAGESIZE, newPage.offset, PAGESIZE.toInt())
                transactionInfo.undoMap[pageNo] = newPage.number
                file.setLong(pageNo * PAGESIZE + CHANGED_BY_TRA_INDEX, transactionInfo.traNum)
            }
        }
    }

    override fun getByte(idx: Long) = b.getByte(convertOffset(idx))

    private fun convertOffset(idx: Long): Long {
        var pageNo = (idx / PAGESIZE).toInt()

        val transactionInfo = MVCC.current()
        if (transactionInfo != null) {
            var lastTra = transactionInfo.traNum
            var maxChange = Long.MAX_VALUE
            do {
                val lastPageChanger = file.getLong(pageNo * PAGESIZE + CHANGED_BY_TRA_INDEX)
                if (lastPageChanger > transactionInfo.traNum) {
                    val tinfo = MVCC[lastPageChanger]
                    if (tinfo == null)
                        throw AssertionError("tinfo not found for reading preImage")
                    val pno = tinfo.undoMap[pageNo]
                    if (pno == null)
                        throw AssertionError("expected page to be found in undomap of transaction")
                    pageNo = pno
                } else {
                    return pageNo * PAGESIZE + idx % PAGESIZE
                }
                if (lastPageChanger >= maxChange) {
                    throw AssertionError("not monoton distribution of traNums to preImages")
                }
                maxChange = lastPageChanger
            } while (true)
        }
        return idx
    }

    override fun setByte(idx: Long, i: Byte) {
        copyPageIfNecessary(idx)
        b.setByte(convertOffset(idx), i)
    }

    override fun getChar(idx: Long) = b.getChar(idx)
    override fun setChar(idx: Long, c: Char) {
        copyPageIfNecessary(idx)
        b.setChar(convertOffset(idx), c)
    }

    override fun getShort(idx: Long) = b.getShort(convertOffset(idx))
    override fun setShort(idx: Long, i: Short) {
        copyPageIfNecessary(idx)
        b.setShort(convertOffset(idx), i)
    }

    override fun getInt(idx: Long) = b.getInt(convertOffset(idx))
    override fun setInt(idx: Long, i: Int) {
        copyPageIfNecessary(idx)
        b.setInt(convertOffset(idx), i)
    }

    override fun getLong(idx: Long) = b.getLong(convertOffset(idx))
    override fun setLong(idx: Long, i: Long) {
        copyPageIfNecessary(idx)
        b.setLong(convertOffset(idx), i)
    }

    override fun getFloat(idx: Long) = b.getFloat(convertOffset(idx))
    override fun setFloat(idx: Long, f: Float) {
        copyPageIfNecessary(idx)
        b.setFloat(convertOffset(idx), f)
    }

    override fun getDouble(idx: Long) = b.getDouble(convertOffset(idx))
    override fun setDouble(idx: Long, f: Double) {
        copyPageIfNecessary(idx)
        b.setDouble(convertOffset(idx), f)
    }

    override fun getBoolean(idx: Long): Boolean = getByte(idx) != 0.toByte()
    override fun setBoolean(idx: Long, b: Boolean) = setByte(idx, (if (b) 1 else 0).toByte())

    override fun getByteArray(idx: Long, ba: ByteArray) = b.getByteArray(idx, ba)
    override fun setByteArray(idx: Long, ba: ByteArray) {
        copyPageIfNecessary(idx)
        b.setByteArray(convertOffset(idx), ba)
    }

    override fun move(from: Long, to: Long, size: Int) {
        b.move(convertOffset(from), convertOffset(to), size)
    }

    override fun newPage(): NioPageFilePage {
        return file.newPage()
    }

    override fun freePage(page: NioPageFilePage) {
        val transactionInfo = MVCC.current()
        if (transactionInfo != null) {
            transactionInfo.freedPages.add(page.number)
        } else {
            file.freePage(page)
        }
    }

    override fun isUsed(pageNum: Int): Boolean {
        val transactionInfo = MVCC.current()
        if (transactionInfo != null) {
            if (transactionInfo.freedPages.contains(pageNum))
                return false
        }
        return file.isUsed(pageNum)
    }

    override fun isFree(pageNum: Int): Boolean {
        return !isUsed(pageNum)
    }

}



fun main(args: Array<String>) {
    val m = MMapper("/tmp/test.bin", 100000L)
    m.putInt(1L, 100)
    assert(m.getInt(1L) == 100)
}