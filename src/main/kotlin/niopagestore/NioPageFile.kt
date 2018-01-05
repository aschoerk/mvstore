/**
 * @author aschoerk
 */

package niopageobjects

import niopageentries.NioPageEntry
import niopagestore.MMapper
import org.agrona.concurrent.MappedResizeableBuffer
import kotlin.experimental.and

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

class NioPageIndexEntry(val entryOffset: Long) {
    fun validate(page: NioPageFilePage) {
        if (!isValid(page))
            throw IndexOutOfBoundsException("trying to use entry not in this page")
    }

    fun isValid(page: NioPageFilePage): Boolean {
        val indexPtrOc = (entryOffset - page.offset >= page.END_OF_HEADER
                && entryOffset - page.offset < page.file.getInt(page.offset + page.AFTER_ELEMENT_INDEX)
                || (entryOffset - page.END_OF_HEADER) % page.INDEX_ENTRY_SIZE == 0L)
        if (indexPtrOc) {
            val indexEntry = NioPageFilePage.IndexEntry(page, entryOffset)
            return indexEntry.offs < PAGESIZE && indexEntry.offs + indexEntry.len <= PAGESIZE
        }
        return false
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as NioPageIndexEntry

        if (entryOffset != other.entryOffset) return false

        return true
    }

    override fun hashCode(): Int {
        return entryOffset.hashCode()
    }

    override fun toString(): String {
        return "NioPageIndexEntry(offset=0x${entryOffset.toString(16)})"
    }


}


class NioPageFilePage(val file: NioPageFile, val offset: Long) {
    init {
        val pageNumber = (offset / PAGESIZE).toInt()
        assert(file.freeMap.findDescription(pageNumber).getUsed(pageNumber), {"trying to create already freed page"})
    }
    constructor(file: NioPageFile, number: Int) : this(file, number.toLong() * PAGESIZE)
    val number
        get() = (offset / PAGESIZE).toInt()
    val END_OF_HEADER = 24          // size of the header information
    val FLAGS_INDEX = 2             // flags about the page bit 30 is always set
    val FREE_ENTRY_INDEX = 0
    val FREE_SPACE_INDEX = 4        // stores the amount of freespace in this pages payload area, be aware,
    // that for each element allocation a 4 Byte index entry is necessary.
    val AFTER_ELEMENT_INDEX = 8     // stores the entryOffset, where the element-index ends
    val BEGIN_OF_PAYLOAD_POS_INDEX = 12  // stores the beginning of the payload
    val CHANGED_BY_TRA_INDEX = 16   // stores information about who changed the page last
    val INDEX_ENTRY_SIZE = 4

    init {
        if (file.getInt(offset) == 0) {
            // automatically initialize when coming from freespace
            file.setShort(offset + FLAGS_INDEX, 0x4000)
            file.setShort(offset + FREE_ENTRY_INDEX, 0)
            file.setInt(offset + FREE_SPACE_INDEX, PAGESIZE.toInt() - END_OF_HEADER)
            file.setInt(offset + AFTER_ELEMENT_INDEX, END_OF_HEADER)  // no payload there, ends immediately after the header
            file.setInt(offset + BEGIN_OF_PAYLOAD_POS_INDEX, PAGESIZE.toInt())  // no payload there, set after the page
            file.setLong(offset + CHANGED_BY_TRA_INDEX, 0L)
        }
    }

    class IndexEntry(val idx: Short,
                     var offs: Int, // offset relativ to page begin
                     var len: Int,  // length of entry if deleted: if <> 0 then space has not been claimed by compaction yet
                     var deleted: Boolean, // entry is deleted, but entry might yet be used as address
                     var canBeReused: Boolean  // entry is deleted and client allows that entry may be reused
    ) {
        constructor(idx: Short, offs: Int, len: Int, deleted: Boolean) : this(idx, offs, len, deleted, false)

        constructor(idx: Short, elementIndexValue: Int)
                : this(idx,
                elementIndexValue ushr 17,
                elementIndexValue and 0x7FFF,
                elementIndexValue and 0x8000 != 0,
                elementIndexValue and 0x10000 != 0)

        constructor(page: NioPageFilePage, offset: Long)
                : this(((offset - page.END_OF_HEADER - page.offset) / page.INDEX_ENTRY_SIZE).toShort(), page.file.getInt(offset))

        constructor(page: NioPageFilePage, idx: Short)
                : this(idx, page.file.getInt(page.offset + page.END_OF_HEADER + idx * page.INDEX_ENTRY_SIZE))

        constructor(page: NioPageFilePage, entry: NioPageIndexEntry) : this(page, entry.entryOffset)

        val value
            get() =(offs shl 17) or len or (if (deleted) 0x8000 else 0x0000) or (if (canBeReused) 0x10000 else 0x00000)

        fun setInPage(page: NioPageFilePage) {
            page.file.setInt(page.offset + page.END_OF_HEADER + idx * page.INDEX_ENTRY_SIZE, value)
        }

        fun offsetInFile(page: NioPageFilePage) = offs + page.offset

        override fun toString(): String {
            return "IndexEntry(idx=$idx, offs=$offs, len=$len, deleted=$deleted, canBeReused=$canBeReused)"
        }


    }

    private fun getIndexEntry(idx: Short) : IndexEntry {
        if (idx * INDEX_ENTRY_SIZE + END_OF_HEADER >= afterElementIndexPos)
            throw IndexOutOfBoundsException("getting index entry")
        return IndexEntry(this, idx)
    }


    fun indexEntries() : Iterator<IndexEntry> {
        return object : Iterator<IndexEntry> {
            private var current = 0
            override fun hasNext(): Boolean = current < (file.getInt(offset + AFTER_ELEMENT_INDEX) - END_OF_HEADER) / INDEX_ENTRY_SIZE

            override fun next(): IndexEntry {
                if (hasNext()) {
                    current++
                    return getIndexEntry((current-1).toShort())
                } else
                    throw IndexOutOfBoundsException("iterate Entries")

            }
        }
    }

    fun countEntries() : Int {
        var result = 0
        entries().forEach { result++ }
        return result
    }

    fun entries() : Iterator<NioPageIndexEntry> {
        return object : Iterator<NioPageIndexEntry> {
            var current = 0.toShort()

            override fun hasNext(): Boolean {
                while (current < (file.getInt(offset + AFTER_ELEMENT_INDEX) - END_OF_HEADER) / INDEX_ENTRY_SIZE
                    && getIndexEntry(current).deleted) current++

                return current < (file.getInt(offset + AFTER_ELEMENT_INDEX) - END_OF_HEADER) / INDEX_ENTRY_SIZE
            }

            override fun next(): NioPageIndexEntry {
                if (hasNext()) {
                    current++
                    return NioPageIndexEntry( offset + END_OF_HEADER + (current - 1) * INDEX_ENTRY_SIZE)
                } else {
                    throw IndexOutOfBoundsException(" No Indexentry left")
                }

            }


        }
    }

    fun offset(entry: NioPageIndexEntry) = IndexEntry(this, entry).offsetInFile(this)

    fun checkDataPage() : String {
        val result = StringBuffer()
        if (!file.freeMap.findDescription(number).getUsed(number)) {
            result.append("page($number): expected to be used\n")
        }
        if ((file.getShort(offset +2) and 0x4000) == 0.toShort()) {
            result.append("page($number): expected 0x4000 at offset 2\n")
        }
        val beginOfPayload = file.getInt(offset + BEGIN_OF_PAYLOAD_POS_INDEX)
        val freeEntries = getReusableIndexEntries()
        val afterElementOffset = file.getInt(offset + AFTER_ELEMENT_INDEX)
        val freeSpace = file.getInt(offset + FREE_SPACE_INDEX)
        var actualFreeSpace = beginOfPayload - afterElementOffset
        var actualFreeEntries = 0
        val indexEntries = mutableListOf<IndexEntry>()
        var index = 0.toShort()
        for (i in (this.END_OF_HEADER..afterElementOffset - 4) step 4) {
            val currentValue = file.getInt(offset + i)
            val indexEntry = IndexEntry(index++, currentValue)
            indexEntries.add(indexEntry)
            if (indexEntry.canBeReused)
                actualFreeEntries++
            if (indexEntry.deleted) {
                actualFreeSpace += indexEntry.len
            }
            if (indexEntry.canBeReused != indexEntry.deleted) {
                result.append("page($number): at moment expect to equal canbeReused and Deleted: $indexEntry\n")
            }
            if (indexEntry.len != 0 && (indexEntry.offs < beginOfPayload || indexEntry.offs > PAGESIZE)) {
                result.append("page($number): invalid offset in indexEntry: $indexEntry\n")
            } else {
                if (indexEntry.len < 0) {
                    result.append("page($number): invalid len < 0 in indexEntry: $indexEntry\n")
                } else if (indexEntry.offs != 0 && indexEntry.offs + indexEntry.len  > PAGESIZE) {
                    result.append("page($number): invalid len in indexEntry: $indexEntry\n")
                }
            }
        }
        if (actualFreeSpace != freeSpace) {
            result.append("page($number): thinks it has $freeSpace, but only $actualFreeSpace left\n")
        }
        if (actualFreeEntries != freeEntries.toInt()) {
            result.append("page($number): thinks it has $freeEntries free Entries, but only $actualFreeEntries left\n")
        }
        indexEntries.sortBy { -it.offs }
        var lastOffset = PAGESIZE.toInt()
        for (e in indexEntries) {
            if (e.offs != 0 && e.len != 0) {
                if (e.offs + e.len > lastOffset) {
                    result.append("page($number): invalid len in indexEntry: $e > $lastOffset\n")
                }
                lastOffset = e.offs
            }
        }
        return result.toString()
    }

    // allocate space in the page if possible
    private fun allocate(length: Short): IndexEntry {
        if (!enoughContinousFreespace((length + if (getReusableIndexEntries() > 0) 0 else INDEX_ENTRY_SIZE).toShort())) {
            compactPayloadArea()
        }
        val proposedOffset = newBeginOfPayloadArea(length)
        if (getReusableIndexEntries() == 0.toShort()) {
            return allocateWithNewIndexEntry(proposedOffset, length)
        } else {
            val result = allocateAndReuseIndexEntry(proposedOffset, length)
            if (result == null)
                return allocateWithNewIndexEntry(proposedOffset, length)
            else
                return result
        }

    }

    private fun allocateAndReuseIndexEntry(proposedOffset: Int, length: Short): IndexEntry? {
        assert(getReusableIndexEntries() > 0)
        for (e in indexEntries()) {
            if (e.canBeReused && e.len == 0) {
                file.setInt(offset + BEGIN_OF_PAYLOAD_POS_INDEX, proposedOffset)        // pre allocate space below the other allocated space
                // maintain index
                e.offs = proposedOffset
                e.len = length.toInt()
                e.deleted = false
                e.canBeReused = false
                e.setInPage(this)
                decReusableIndexEntries()
                addToFreeSpace(-length)  // entry is reused so it does not count into freespace
                return e
            }
        }
        return null
    }

    private fun allocateWithNewIndexEntry(proposedOffset: Int, length: Short): IndexEntry {
        val currentAfterElementIndexPos = afterElementIndexPos
        assert(proposedOffset >= currentAfterElementIndexPos + 4, { "proposedOffset: $proposedOffset >= ${currentAfterElementIndexPos + 4}" })
        // maintain header information
        val result = IndexEntry(((currentAfterElementIndexPos - END_OF_HEADER) / INDEX_ENTRY_SIZE).toShort(), proposedOffset, length.toInt(), false)
        file.setInt(offset + AFTER_ELEMENT_INDEX, currentAfterElementIndexPos + INDEX_ENTRY_SIZE)  // make element index 1 entry longer
        file.setInt(offset + BEGIN_OF_PAYLOAD_POS_INDEX, proposedOffset)        // pre allocate space below the other allocated space
        addToFreeSpace(-length - INDEX_ENTRY_SIZE)  // need to remove index_entry_size as well because of lengthening of indexentrytable
        // maintain index
        result.setInPage(this)
        return result
    }

    private fun addToFreeSpace(length: Int) {
        file.setInt(offset + FREE_SPACE_INDEX, file.getInt(offset + FREE_SPACE_INDEX) + length) // maintain freespace info
    }

    fun compactIndexArea() {
        val entries = mutableListOf<IndexEntry>()
        indexEntries().forEach {
            if (it.deleted && !it.canBeReused)
                throw AssertionError(" can't compact index on fixed page")
            entries.add(it)
        }
        var indexOffset = END_OF_HEADER
        for (e in entries) {
            if (!e.deleted || e.len != 0) {
                file.setInt(offset + indexOffset, e.value)
                indexOffset += INDEX_ENTRY_SIZE
            } else {
                decReusableIndexEntries()
                addToFreeSpace(INDEX_ENTRY_SIZE)
            }
        }
        file.setInt(offset + AFTER_ELEMENT_INDEX, indexOffset)
    }

    private fun compactPayloadArea() {
        var toMove = 0
        val entries = mutableListOf<IndexEntry>()
        indexEntries().forEach { if (it.len > 0) entries.add(it) }
        entries.sortBy { -it.offs }

        entries.forEach {
            if (it.deleted && it.len > 0) {
                toMove += it.len
                it.len = 0
                it.setInPage(this)
            } else {
                if (toMove > 0) {
                    file.move(offset + it.offs, offset + it.offs + toMove, it.len)
                    it.offs += toMove
                    it.setInPage(this)
                }
            }
        }
        file.setInt(offset + BEGIN_OF_PAYLOAD_POS_INDEX, file.getInt(offset + BEGIN_OF_PAYLOAD_POS_INDEX) + toMove)
    }

    private fun enoughContinousFreespace(length: Short) =
            newBeginOfPayloadArea(length) >= afterElementIndexPos + INDEX_ENTRY_SIZE

    private val afterElementIndexPos
        get() = file.getInt(offset + AFTER_ELEMENT_INDEX)

    private fun newBeginOfPayloadArea(length: Short) = file.getInt(offset + BEGIN_OF_PAYLOAD_POS_INDEX) - length

    fun allocationFitsIntoPage(length: Int) = allocationFitsIntoPage(length.toShort())

    fun allocationFitsIntoPage(length: Short) =
            freeSpace() >=
                    length +
                        if (getReusableIndexEntries() > 0)
                            0
                        else INDEX_ENTRY_SIZE

    fun freeSpace() = file.getInt(offset + FREE_SPACE_INDEX)

    fun add(entry: NioPageEntry): NioPageIndexEntry {
        if (!allocationFitsIntoPage(entry.length))
            throw IndexOutOfBoundsException("can't add new entry in this page")
        val indexEntry = allocate(entry.length)

        if (indexEntry != null)
            entry.marshalTo(file,  offset + indexEntry.offs)
        return NioPageIndexEntry(offset + END_OF_HEADER + indexEntry.idx * INDEX_ENTRY_SIZE)
    }

    fun removeButKeepIndexEntry(entry: NioPageIndexEntry): Unit {
        val entry = IndexEntry(this, entry.entryOffset)
        if (entry.offs == 0)
            throw IndexOutOfBoundsException("entry to be deleted is already removed")
        if (entry.deleted)
            throw IndexOutOfBoundsException("entry to be deleted is already")
        entry.deleted = true
        entry.setInPage(this)
        addToFreeSpace(entry.len)
    }

    fun remove(entry: NioPageIndexEntry): Unit {
        val entry = IndexEntry(this, entry.entryOffset)
        remove(entry)
    }

    fun remove(entry: IndexEntry) {
        if (entry.offs == 0)
            throw IndexOutOfBoundsException("entry to be removed is already")
        if (entry.deleted)
            throw IndexOutOfBoundsException("entry to be deleted is already")
        for (e in indexEntries()) {
            if (e.offs == entry.offs && !e.deleted) {
                assert(e.idx == entry.idx)
                break
            }
        }
        addToFreeSpace(entry.len)
        entry.deleted = true
        entry.canBeReused = true // means: can be reused
        incReusableIndexEntries()
        entry.setInPage(this)
    }

    fun empty() : Boolean = indexEntries().asSequence().fold(true) { isEmpty, element -> isEmpty && element.deleted }


    private fun clrReusableIndexEntries() {
        file.setShort(offset + FREE_ENTRY_INDEX, 0)
    }

    private fun decReusableIndexEntries() {
        file.setShort(offset + FREE_ENTRY_INDEX, (getReusableIndexEntries() - 1).toShort())
    }

    private fun incReusableIndexEntries() {
        file.setShort(offset + FREE_ENTRY_INDEX, (getReusableIndexEntries() + 1).toShort())
    }

    private fun getReusableIndexEntries() = file.getShort(offset + FREE_ENTRY_INDEX)


    operator override fun equals(other: Any?): Boolean {
        if (super.equals(other))
            return true
        return other is NioPageFilePage && other.file == file && other.offset == offset
    }

    override fun hashCode(): Int {
        return offset.hashCode()
    }


}


class NioPageFile(val buffer: MappedResizeableBuffer, val length: Long) : NioBufferWithOffset(buffer, 0) {
    val FREESPACE_OFFSET = PAGESIZE
    val HEADER_OFFSET = 0
    val ROOT_PAGE_OFFSET = 32L

    var rootPage
        get() = getInt(offset + ROOT_PAGE_OFFSET)
        set(value) = setInt(offset + ROOT_PAGE_OFFSET, value)

    val freeMap = FreeSpace(this)

    fun pageOffset(page: Long) = PAGESIZE * page


    fun newPage() = NioPageFilePage(this, freeMap.newPage())

    fun freePage(page: NioPageFilePage) = freeMap.freePage(page.offset)

    fun usedPagesIterator() = freeMap.usedPagesIterator()



}

const val PAGESIZE = 8192L
const val START_OF_MAP = 16L
const val FREEMAP_MAGIC = 0x16578954
const val PAGE_MAGIC = -0x12568762
const val MIN_INITIAL_SIZE = PAGESIZE * 16 * 8


class FreeSpace(val file: NioPageFile) {

    class FreespaceDescription(val file: NioPageFile,
                               val offset: Long   // start of managed Space
    ) {
        // number of pages managed by this FreespaceDescription
        val pagesCovered: Int
            get() = minOf(file.length / PAGESIZE, (file.getInt(offset + 1) * PAGESIZE * 8)).toInt()
        // support linking of freespace areas using this pointer
        val nextFreeSpaceOffset: Long
            get() = file.getLong(offset + 8)
        // number of pages used from file by this FreespaceDescription
        val pagesUsed: Int
            get() = ((pagesCovered + START_OF_MAP * 8L) / (PAGESIZE * 8)).toInt()
        val firstOffset: Long
            get() = offset
        val behindFreespaceOffset: Long
            get() = offset + pagesCovered * PAGESIZE
        val firstPageManaged: Int
            get() = (offset / PAGESIZE).toInt()

        val managementPages: Int
            get() = ((nextFreeSpaceOffset - offset) / (PAGESIZE * 8)).toInt()

        fun set(usedPage: Int, usedOrNot: Boolean) {
            val pageNumInFreeSpace = (usedPage * PAGESIZE - offset) / PAGESIZE
            if (pageNumInFreeSpace < 0 || pageNumInFreeSpace > pagesCovered)
                throw IndexOutOfBoundsException("trying to figure out freespace")
            val byteOffsetInMap = pageNumInFreeSpace / 8 + START_OF_MAP
            val bitPos = pageNumInFreeSpace % 8
            val bit: Int = (1 shl bitPos.toInt())
            val b: Int = file.getByte(byteOffsetInMap + offset).toInt()
            if (bit and b != 0 && usedOrNot || (b and bit) == 0 && !usedOrNot) {
                throw IndexOutOfBoundsException("Invalid setting of freespace")
            }
            file.setByte(byteOffsetInMap + offset, (b xor bit).toByte())
        }

        fun getUsed(usedPage: Int) : Boolean {
            val pageNumInFreeSpace = (usedPage * PAGESIZE - offset) / PAGESIZE
            if (pageNumInFreeSpace < 0 || pageNumInFreeSpace > pagesCovered)
                throw IndexOutOfBoundsException("trying to figure out freespace")
            val byteOffsetInMap = pageNumInFreeSpace / 8 + START_OF_MAP
            val bitPos = pageNumInFreeSpace % 8
            val bit: Int = (1 shl bitPos.toInt())
            val b: Int = file.getByte(byteOffsetInMap + offset).toInt()
            return bit and b != 0
        }

        enum class PAGEINFO { FREE, ALLOCATED, FREESPACE }

        // page is pagenumber in file
        fun pageInfo(page: Int): PAGEINFO {
            val pageNumInFreeSpace = (page * PAGESIZE - offset) / PAGESIZE
            if (pageNumInFreeSpace < 0 || pageNumInFreeSpace > pagesCovered)
                throw IndexOutOfBoundsException("trying to figure out freespace")
            if (pageNumInFreeSpace <= managementPages)
                return PAGEINFO.FREESPACE
            val byteOffsetInMap = pageNumInFreeSpace / 8 + START_OF_MAP
            val bitPos = pageNumInFreeSpace % 8
            val bit: Int = (1 shl bitPos.toInt())
            val b: Int = file.getByte(byteOffsetInMap + offset).toInt()
            return if (bit and b != 0)
                PAGEINFO.ALLOCATED
            else
                PAGEINFO.FREE
        }

        /**
         * returns
         *  byteoffset of freepage, 0 if no free page found
         *
         */
        fun findAndSetPage(): Long {
            for (bp in START_OF_MAP..START_OF_MAP + pagesCovered / 8 step 4) {
                val content = file.getInt(offset + bp)
                if (content != -1) {
                    var mask = 0x1
                    for (i in 0..31) {
                        if (content and mask == 0) {
                            val pageNumber = (bp - START_OF_MAP) * 8 + i + firstPageManaged
                            assert (pageNumber <= Int.MAX_VALUE)
                            set(pageNumber.toInt(), true)
                            return pageNumber * PAGESIZE
                        }
                        mask = mask shl 1
                    }
                    throw AssertionError("should not reach here")
                }
            }
            return 0L
        }

    }

    private val freespaceDescriptions: MutableList<FreespaceDescription> = mutableListOf()

    init {
        if (file.getInt(file.FREESPACE_OFFSET) != FREEMAP_MAGIC) synchronized(file) {
            val freespaceOffset = file.FREESPACE_OFFSET
            freespaceDescriptions.add(initNextFreeSpace(freespaceOffset, file.length))
        } else {
            var currentOffset = file.FREESPACE_OFFSET
            do {
                val desc = FreespaceDescription(file, currentOffset)
                freespaceDescriptions.add(desc)
                currentOffset = desc.nextFreeSpaceOffset
            } while (currentOffset != 0L)
        }
    }

    fun findDescription(page: Int): FreespaceDescription {
        var pageCount = page
        for (d in freespaceDescriptions) {
            pageCount -= d.pagesCovered
            if (pageCount <= 0)
                return d
        }
        throw IndexOutOfBoundsException("could not find freepageentry")
    }


    private fun initNextFreeSpace(freespaceOffset: Long, proposedLength: Long): FreespaceDescription {
        val size = minOf(MIN_INITIAL_SIZE, proposedLength)
        file.setInt(freespaceOffset, FREEMAP_MAGIC)
        file.setInt(freespaceOffset + 4, (size / (PAGESIZE * 8) + 1).toInt())
        file.setLong(freespaceOffset + 8, 0L)
        val result = FreespaceDescription(file, freespaceOffset)

        // initialize map so that own pages are used
        for (i in 0..result.pagesUsed) {
            result.set((freespaceOffset / PAGESIZE).toInt() + i, true)
        }
        return result
    }

    fun newPage(): Long {
        for (d in freespaceDescriptions) {
            val freeOffset = d.findAndSetPage()
            if (freeOffset != 0L) {
                file.setInt(freeOffset, 0)  // make sure this page will get initialized as soon as first used.
                return freeOffset
            }
        }
        return 0L
    }

    fun freePage(offset: Long): Unit {
        for (d in freespaceDescriptions) {
            if (d.firstOffset <= offset && d.behindFreespaceOffset > offset) {
                d.set((offset / PAGESIZE).toInt(), false)
                return
            }
        }
        throw IndexOutOfBoundsException("no page existent in File")
    }

    class UsedPagesIterator<T>(val file: NioPageFile, val freespaceDescriptions: List<FreespaceDescription>) : Iterator<NioPageFilePage> {
        var currentFreeSpaceDescription = 0
        var pageInFreeSpaceDescription = 0

        init {
            initForFreeSpaceDescription()
        }

        fun initForFreeSpaceDescription(): Unit {
            pageInFreeSpaceDescription = 0
        }

        override fun hasNext(): Boolean {
            while (currentFreeSpaceDescription < freespaceDescriptions.size) {

                val freeSpaceDescr = freespaceDescriptions[currentFreeSpaceDescription]
                while (pageInFreeSpaceDescription < freeSpaceDescr.pagesCovered) {
                    if (freeSpaceDescr.pageInfo(pageInFreeSpaceDescription + freespaceDescriptions[currentFreeSpaceDescription].firstPageManaged)
                            == FreespaceDescription.PAGEINFO.ALLOCATED)
                        return true
                    else
                        pageInFreeSpaceDescription++
                }
                currentFreeSpaceDescription++

                if (currentFreeSpaceDescription < freespaceDescriptions.size)
                    initForFreeSpaceDescription()


            }
            return false
        }

        override fun next(): NioPageFilePage {
            if (hasNext()) {
                val result = freespaceDescriptions[currentFreeSpaceDescription].firstPageManaged + pageInFreeSpaceDescription
                pageInFreeSpaceDescription++
                return NioPageFilePage(file, result * PAGESIZE)
            }
            throw IndexOutOfBoundsException("no more pages can be found")
        }

    }

    fun usedPagesIterator(): Iterator<NioPageFilePage> {
        return UsedPagesIterator<NioPageFilePage>(file, freespaceDescriptions)
    }
}

fun main(args: Array<String>) {
    val m = MMapper("/tmp/test.bin", 100000L)
    m.putInt(1L, 100)
    assert(m.getInt(1L) == 100)
}