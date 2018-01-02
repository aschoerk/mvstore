package niopagestore

import nioobjects.TXIdentifier
import niopageentries.ByteArrayPageEntry
import niopageentries.DoublePageEntry
import niopageentries.EmptyPageEntry
import niopageobjects.NioPageFile
import org.agrona.concurrent.MappedResizeableBuffer
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.RandomAccessFile

/**
 * @author aschoerk
 */
class NioBtreeTest {
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
    fun simpleBTreeTest() {
        val filep = file
        if (filep == null)
            throw AssertionError("")
        val tree = NioBTree(filep)
        val value = EmptyPageEntry()
        for (i in 1..100) {
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
        for (i in 1..100) {
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
    }

    @Test
    fun simpleBTreeTestWithValue() {
        val filep = file
        if (filep == null)
            throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..3) {
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
        for (i in 1..3) {
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
    }

    @Test
    fun simpleBTreeTestOneSplit() {
        val filep = file
        if (filep == null)
            throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..5) {
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
        for (i in 1..5) {
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
    }

}