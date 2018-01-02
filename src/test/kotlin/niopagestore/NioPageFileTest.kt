package niopagestore

import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import niopageentries.DoublePageEntry
import niopageobjects.NioPageFile
import niopageobjects.NioPageFilePage
import niopageobjects.NioPageIndexEntry
import niopageobjects.PAGESIZE
import org.agrona.concurrent.MappedResizeableBuffer
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.RandomAccessFile

class NioPageFileTest {
    private var file: NioPageFile? = null

    private var randomAccessFile: RandomAccessFile? = null

    @Before fun setupNioPageFileTest() {
        File("/tmp/testfile.bin").delete()
        val f = RandomAccessFile("/tmp/testfile.bin", "rw")
        f.seek(65 * 8192 - 1)
        f.writeByte(0xFF)
        val b = MappedResizeableBuffer(f.channel,0L,f.length() )
        this.file = NioPageFile(b, f.length())
        this.randomAccessFile = f
    }

    @Test
    fun testInitClear()  {
        
        val pages: MutableList<NioPageFilePage> = mutableListOf()
        for (i in 0..randomAccessFile!!.length() / PAGESIZE - 3) {
            val page = file!!.newPage()
            pages.add(page)
        }
        val freedPages: MutableList<NioPageFilePage> = mutableListOf()


        for (p in pages) {
            file!!.freePage(p)
            freedPages.add(p)
            for (l in file!!.usedPagesIterator()) {
                assertTrue(pages.contains(l))
                assertFalse(freedPages.contains(l))
            }
        }
    }
    
    @Test
    fun canAllocateAndDeleteOneEntryInPage() {
        val page = file!!.newPage()

        val e = DoublePageEntry(1.111)
        assertTrue (page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 3))
        val idx = page.add(e)
        idx.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt()- (e.length + 4) - page.END_OF_HEADER - 3))
        page.delete(idx)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - 4 - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - 4 - page.END_OF_HEADER - 3))
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
        assertTrue (page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 3))
        val idx1 = page.add(e)
        idx1.validate(page)
        val idx2 = page.add(e)
        idx2.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - page.END_OF_HEADER - 3))
        page.delete(idx2)
        page.delete(idx1)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - 8 - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - 8 - page.END_OF_HEADER - 3))
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
        assertTrue (page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 3))
        val idx = page.add(e)
        idx.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt()- (e.length + 4) - page.END_OF_HEADER - 3))
        page.remove(idx)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt()  - page.END_OF_HEADER - 3))
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
        assertTrue (page.allocationFitsIntoPage(e.length))
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - page.END_OF_HEADER - 3))
        val idx1 = page.add(e)
        idx1.validate(page)
        val idx2 = page.add(e)
        idx2.validate(page)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() - (e.length + 4) * 2 - page.END_OF_HEADER - 3))
        page.remove(idx2)
        page.remove(idx1)
        assertTrue(page.allocationFitsIntoPage(PAGESIZE.toInt()  - 4 - page.END_OF_HEADER - 4))
        assertFalse(page.allocationFitsIntoPage(PAGESIZE.toInt() -  4 - page.END_OF_HEADER - 3))
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
        val maxEntries = (PAGESIZE.toInt() - page.END_OF_HEADER) / (e.length + 4)
        for (i in 1..maxEntries) {
            val element = page.add(e)
            element.validate(page)
            entries.add(element)
        }
        assertFalse(page.allocationFitsIntoPage(e.length))
        for (e in page.entries()) {
            e.validate(page)
            assertTrue(entries.contains(e))
        }
        var count = 0;
        for (pageIndexEntry in entries.filter { it.entryOffset and 8L == 8L }) {
            page.delete(pageIndexEntry)
            assertFalse(page.entries().asSequence().contains(pageIndexEntry))
            count++
            assertTrue(page.allocationFitsIntoPage(e.length * count))
            assertFalse(page.allocationFitsIntoPage(e.length * (count + 1)))
        }
        assertTrue(page.allocationFitsIntoPage(count * e.length))
        assertFalse(page.allocationFitsIntoPage((count +1)* e.length))
        val newMaxEntries = maxEntries - (count * 4 / (e.length + 4) + 1)  // leftover entries
        assertFalse(page.entries().asSequence().any { it.entryOffset and 8L == 8L })
        assertTrue(page.entries().asSequence().all { it.isValid(page)})
        val entries2  = mutableListOf<NioPageIndexEntry>()
        entries2.addAll(entries.filter{it.entryOffset and 8L != 8L})

        for (i in 1..newMaxEntries - (maxEntries - count)) {
            val element = page.add(e)
            element.validate(page)
            entries2.add(element)
        }
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
        val maxEntries = (PAGESIZE.toInt() - page.END_OF_HEADER) / (e.length + 4)
        for (i in 1..maxEntries) {
            val element = page.add(e)
            element.validate(page)
            entries.add(element)
        }
        assertFalse(page.allocationFitsIntoPage(e.length))
        for (e in page.entries()) {
            e.validate(page)
            assertTrue(entries.contains(e))
        }
        var count = 0;
        for (pageIndexEntry in entries.filter { it.entryOffset and 8L == 8L }) {
            page.remove(pageIndexEntry)
            assertFalse(page.entries().asSequence().contains(pageIndexEntry))
            count++
            assertTrue(page.allocationFitsIntoPage(e.length * count))
            assertFalse(page.allocationFitsIntoPage(e.length * (count + 1)))
        }
        assertFalse(page.entries().asSequence().any { it.entryOffset and 8L == 8L })
        assertTrue(page.entries().asSequence().all { it.isValid(page)})
        val entries2  = mutableListOf<NioPageIndexEntry>()
        entries2.addAll(entries.filter{it.entryOffset and 8L != 8L})

        for (i in 1..count) {
            val element = page.add(e)
            element.validate(page)
            entries2.add(element)
        }
        assertFalse(page.allocationFitsIntoPage(e.length + page.INDEX_ENTRY_SIZE))
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
}