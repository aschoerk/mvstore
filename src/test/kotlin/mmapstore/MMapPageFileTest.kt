package mmapstore

import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.agrona.concurrent.MappedResizeableBuffer
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.RandomAccessFile

class TransactionalMMapPageFileTest : MMapPageFileTest() {

    @Before
    fun setupTransactionalMMapPageFileTest() {
        println("test")
        MVCC.begin()
        this.file = MVCCFile(this.file!!)
    }

    @After
    fun endOfTransactionalMMapPageFileTest() {
        MVCC.commit()
    }

    @Test
    override fun testInitClear() {

    }

    override fun reTra() {
        MVCC.commit()
        MVCC.begin()
    }
}

open class MMapPageFileTest {
    protected var file: IMMapPageFile? = null

    private var randomAccessFile: RandomAccessFile? = null

    @Before
    fun setupNioPageFileTest() {
        File("/tmp/testfile.bin").delete()
        val f = RandomAccessFile("/tmp/testfile.bin", "rw")
        f.seek(10000 * 8192 - 1)
        f.writeByte(0xFF)
        val b = MappedResizeableBuffer(f.channel, 0L, f.length())
        this.file = MMapPageFile(b, f.length())
        this.randomAccessFile = f
    }

    open fun reTra() {

    }


    @Test
    open fun testInitClear() {

        val pages: MutableList<MMapPageFilePage> = mutableListOf()
        var lastPage: MMapPageFilePage? = null
        for (i in 0..(randomAccessFile!!.length() / PAGESIZE - 1) - 3) {
            val page = file!!.newPage()
            if (lastPage != null) {
                if (lastPage.offset + 8192 != page.offset) {
                    assertTrue(lastPage.offset + 2 * 8192 == page.offset)
                }
            }
            pages.add(page)
            lastPage = page
        }
        val freedPages: MutableList<MMapPageFilePage> = mutableListOf()


        val realFile = file as MMapPageFile
        for (p in pages) {
            realFile!!.freePage(p)
            freedPages.add(p)
            if (p.number % 1000 == 0) {
                println("iterating through pages")
                for (l in realFile!!.usedPagesIterator()) {
                    assertTrue(pages.contains(l))
                    assertFalse(freedPages.contains(l))
                }
            }
        }
    }

    @Test
    fun canAllocateAndDeleteOneEntryInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        assertTrue(page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 3))
        val idx = page.add(e)
        idx.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) - END_OF_HEADER - 3))
        page.removeButKeepIndexEntry(idx)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - 4 - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - 4 - END_OF_HEADER - 3))
    }

    @Test
    fun canAllocateAndDeleteOneEntryInSecondPage() {
        file!!.newPage()
        canAllocateAndDeleteOneEntryInPage()
    }

    @Test
    fun canAllocateAndDeleteTwoEntriesInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        assertTrue(page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 3))
        val idx1 = page.add(e)
        idx1.validate(page)
        reTra()
        val idx2 = page.add(e)
        idx2.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - END_OF_HEADER - 3))
        page.removeButKeepIndexEntry(idx2)
        page.removeButKeepIndexEntry(idx1)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - 8 - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - 8 - END_OF_HEADER - 3))
    }

    @Test
    fun canAllocateAndDeleteTwoEntriesInSecondPage() {
        file!!.newPage()
        canAllocateAndDeleteTwoEntriesInPage()
    }


    @Test
    fun canAllocateAndRemoveOneEntryInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        assertTrue(page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 3))
        val idx = page.add(e)
        reTra()
        idx.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) - END_OF_HEADER - 3))
        page.remove(idx)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 3))
    }

    @Test
    fun canAllocateAndRemoveOneEntryInSecondPage() {
        file!!.newPage()
        canAllocateAndRemoveOneEntryInPage()
    }

    @Test
    fun canAllocateAndRemoveTwoEntriesInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        assertTrue(page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 3))
        val idx1 = page.add(e)
        reTra()
        idx1.validate(page)
        val idx2 = page.add(e)
        idx2.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - END_OF_HEADER - 3))
        page.remove(idx2)
        page.remove(idx1)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - 4 - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - 4 - END_OF_HEADER - 3))
    }

    @Test
    fun canAllocateAndRemoveTwoEntriesInSecondPage() {
        file!!.newPage()
        canAllocateAndRemoveTwoEntriesInPage()
    }

    @Test
    fun canAllocateAndDeleteAllEntriesInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        val entries = mutableListOf<NioPageIndexEntry>()
        val maxEntries = (PAGESIZE.toInt() - END_OF_HEADER) / (e.length + 4)
        for (i in 1..maxEntries) {
            val element = page.add(e)
            element.validate(page)
            entries.add(element)
        }
        assertFalse(page.allocationFitsIntoPage(e.length))

        reTra()
        for (e in page.entries()) {
            e.validate(page)
            assertTrue(entries.contains(e))
        }
        var count = 0
        for (pageIndexEntry in entries.filter { it.entryOffset and 8L == 8L }) {
            reTra()
            page.removeButKeepIndexEntry(pageIndexEntry)
            assertFalse(page.entries().asSequence().contains(pageIndexEntry))
            count++
            assertTrue(page.allocationFitsIntoPage(e.length * count))
            assertFalse(page.allocationFitsIntoPage(e.length * (count + 1)))
        }
        assertTrue(page.allocationFitsIntoPage(count * e.length))
        assertFalse(page.allocationFitsIntoPage((count + 1) * e.length))
        val newMaxEntries = maxEntries - (count * 4 / (e.length + 4) + 1)  // leftover entries
        assertFalse(page.entries().asSequence().any { it.entryOffset and 8L == 8L })
        assertTrue(page.entries().asSequence().all { it.isValid(page) })
        val entries2 = mutableListOf<NioPageIndexEntry>()
        entries2.addAll(entries.filter { it.entryOffset and 8L != 8L })

        for (i in 1..newMaxEntries - (maxEntries - count)) {
            val element = page.add(e)
            element.validate(page)
            entries2.add(element)
        }
        reTra()

        assertFalse(page.allocationFitsIntoPage(e.length))
        for (e in page.entries()) {
            e.validate(page)
            assertTrue(entries2.contains(e))
        }
    }

    @Test
    fun canAllocateAndDeleteAllEntriesInSecondPage() {
        file!!.newPage()
        canAllocateAndDeleteAllEntriesInPage()
    }

    @Test
    fun canAllocateAndRemoveAllEntriesInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        val entries = mutableListOf<NioPageIndexEntry>()
        val maxEntries = (PAGESIZE.toInt() - END_OF_HEADER) / (e.length + 4)
        for (i in 1..maxEntries) {
            val element = page.add(e)
            element.validate(page)
            entries.add(element)
        }
        reTra()
        assertFalse(page.allocationFitsIntoPage(e.length))
        for (e in page.entries()) {
            e.validate(page)
            assertTrue(entries.contains(e))
        }
        var count = 0
        for (pageIndexEntry in entries.filter { it.entryOffset and 8L == 8L }) {
            page.remove(pageIndexEntry)
            assertFalse(page.entries().asSequence().contains(pageIndexEntry))
            count++
            assertTrue(page.allocationFitsIntoPage(e.length * count))
            assertFalse(page.allocationFitsIntoPage(e.length * (count + 1)))
        }
        reTra()
        assertFalse(page.entries().asSequence().any { it.entryOffset and 8L == 8L })
        assertTrue(page.entries().asSequence().all { it.isValid(page) })
        val entries2 = mutableListOf<NioPageIndexEntry>()
        entries2.addAll(entries.filter { it.entryOffset and 8L != 8L })

        for (i in 1..count) {
            val element = page.add(e)
            element.validate(page)
            entries2.add(element)
        }
        assertFalse(page.allocationFitsIntoPage(e.length + INDEX_ENTRY_SIZE))
        for (e in page.entries()) {
            e.validate(page)
            assertTrue(entries2.contains(e))
        }
    }

    @Test
    fun canAllocateAndRemoveAllEntriesInSecondPage() {
        file!!.newPage()
        canAllocateAndRemoveAllEntriesInPage()
    }

    @Test
    fun canStoreReadAndDeleteOneEntryInPage() {
        val page = file!!.newPage()
        createTestEntries().shuffled().forEach { writeReadInPageCheck(page, it) }
    }

    @Test
    fun canMoveOneEntryInPage() {
        val page = file!!.newPage()
        val dest = file!!.newPage()
        val testEntries = createTestEntries()
        testEntries.shuffled().forEach {
            val idx1 = page.add(it)
        }
        assertTrue(page.countEntries() == testEntries.size)
        assertTrue(dest.countEntries() == 0)
        println("checking original page")
        page.entries().asSequence().toList().sortedBy { it.entryOffset }.forEach {
            println("entry: $it")
            val entry = unmarshalFrom(file!!, it)
            println("entry: $entry")
            assertTrue("original contains: $entry", testEntries.contains(entry))
            dest.add(entry)
            page.remove(it)
        }
        assertTrue(dest.countEntries() == testEntries.size)
        assertTrue(page.countEntries() == 0)
        println("checking copied page")
        dest.entries().asSequence().toList().forEach {
            println("entry: $it")
            val entry = unmarshalFrom(file!!, it)
            println("entry: $entry")
            assertTrue("destination contains: $entry", testEntries.contains(entry))
            page.add(entry)
            dest.remove(it)
        }
        assertTrue("${page.countEntries()} != ${testEntries.size}", page.countEntries() == testEntries.size)
        assertTrue(dest.countEntries() == 0)
        println("checking original restored page")
        page.entries().asSequence().toList().forEach {
            println("entry: $it")
            val entry = unmarshalFrom(file!!, it)
            println("entry: $entry")
            assertTrue("original once more contains: $entry", testEntries.contains(entry))
            page.remove(it)
        }
        assertTrue(page.countEntries() == 0)
        assertTrue(dest.countEntries() == 0)
    }


    private fun createTestEntries(): MutableList<ComparableMMapPageEntry> {
        val testEntries = mutableListOf(
                BytePageEntry(11),
                ShortPageEntry(-23),
                IntPageEntry(-1111112222),
                LongPageEntry(-0x7F1188AA11223344),
                FloatPageEntry(1.11E20F),
                DoublePageEntry(1.1347236747E10),
                CharPageEntry('a'),
                CharPageEntry('ä'),
                CharPageEntry('ß'),
                StringPageEntry("teststring"),
                ByteArrayPageEntry(ByteArray(100))
        )
        testEntries.add(ListPageEntry(testEntries))
        return testEntries
    }

    private fun writeReadInPageCheck(page: MMapPageFilePage, entry: MMapPageEntry) {
        val idx1 = page.add(entry)
        val read = unmarshalFrom(file!!, page.offset(idx1))
        page.remove(idx1)
        assertTrue("entry: $entry, read: $read", entry == read)
    }

    @Test
    fun canAllocateAndMoveTwoEntriesInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        assertTrue(page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - END_OF_HEADER - 3))
        val idx1 = page.add(e)
        idx1.validate(page)
        val idx2 = page.add(e)
        idx2.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - END_OF_HEADER - 3))
    }
}