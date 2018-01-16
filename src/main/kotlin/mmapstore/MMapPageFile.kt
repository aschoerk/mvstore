/**
 * @author aschoerk
 */

package mmapstore

import org.agrona.concurrent.MappedResizeableBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

const val PAGESIZE = 8192L


// information in first page of file

const val FREESPACE_OFFSET = PAGESIZE

// structure and contents of freespace-page
const val START_OF_FREESPACE_MAP = 4L
const val FREEMAP_MAGIC = 0x16578954

// number of data-pages managed by one freespace-region
const val DATAPAGES_PER_FREESPACE_REGION = ((PAGESIZE - START_OF_FREESPACE_MAP) * 8).toInt()
const val PAGES_PER_FREESPACE_REGION = DATAPAGES_PER_FREESPACE_REGION + 1


class FileId

interface IMMapPageFile : IMMapBufferWithOffset {
    fun newPage(): MMapPageFilePage
    fun freePage(page: MMapPageFilePage)
    fun freePage(page: Int)
    fun getPage(page: Int) : MMapPageFilePage
    fun isUsed(pageNum: Int): Boolean
    fun isFree(pageNum: Int): Boolean
    val fileId : FileId
    val lock: ReentrantLock
}


open class MMapPageFile(val buffer: MappedResizeableBuffer, val length: Long) : MMapBufferWithOffset(buffer, 0), IMMapBufferWithOffset, IMMapPageFile {
    override val lock = ReentrantLock()

    override fun freePage(page: Int) {
        setFree(page)
    }

    override fun getPage(page: Int) = MMapPageFilePage(this, page * PAGESIZE)


    override val fileId = FileId()

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


    override fun newPage(): MMapPageFilePage {
        val pageNum = allocPage()
        if (pageNum == null)
            throw AssertionError("TODO: extend file")
        setInt(pageNum * PAGESIZE, 0)    // initialize page to show also by content, that it is new
        return getPage(pageNum)
    }

    override fun freePage(page: MMapPageFilePage) = setFree(page.number)

    fun usedPagesIterator(): Iterator<MMapPageFilePage> {

        return object : Iterator<MMapPageFilePage> {
            var lastPageNumber = 0

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                if (lastPageNumber == 0) {
                    lastPageNumber = 1
                    advance()
                }
                return lastPageNumber * PAGESIZE < this@MMapPageFile.length && this@MMapPageFile.isUsed(lastPageNumber)
            }

            private fun advance() {
                var isUsed = false
                do {
                    lastPageNumber++
                    if ((lastPageNumber - 1) % PAGES_PER_FREESPACE_REGION == 0)
                        lastPageNumber++
                    if (lastPageNumber * PAGESIZE > this@MMapPageFile.length)
                        return
                    isUsed = this@MMapPageFile.isUsed(lastPageNumber)
                } while (!isUsed)
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): MMapPageFilePage {
                if (hasNext()) {
                    val result = this@MMapPageFile.getPage(lastPageNumber)
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



