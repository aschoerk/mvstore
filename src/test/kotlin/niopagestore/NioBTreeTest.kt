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
import java.util.*

/**
 * @author aschoerk
 */
class NioBtreeTest {
    private var file: NioPageFile? = null

    private var randomAccessFile: RandomAccessFile? = null

    @Before fun setupNioPageFileTest() {
        File("/tmp/testfile.bin").delete()
        val f = RandomAccessFile("/tmp/testfile.bin", "rw")
        f.seek(1000 * 8192 - 1)
        f.writeByte(0xFF)
        val b = MappedResizeableBuffer(f.channel,0L,f.length() )
        this.file = NioPageFile(b, f.length())
        this.randomAccessFile = f
    }

    @Test
    fun simpleBTreeTest() {
        val filep = file ?: throw AssertionError("")
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
    fun canStoreDuplicateKeysWithEqualValues() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        for (i in 1..100) {
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
        for (i in 1..100) {
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
        for (i in 1..100) {
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
    }

    @Test
    fun canStoreDuplicateKeysWithDifferentValues() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        for (i in 1..100) {
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry((i+1).toDouble()))
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
        for (i in 1..100) {
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
        for (i in 1..100) {
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), DoublePageEntry((i+1).toDouble()))
        }
    }

    @Test
    fun simpleBTreeTestWithValue() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..3) {
            println("Inserting $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("    $it, ") }
        }
        for (i in 1..3) {
            println("Deleting $i")
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("      $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestReverseDelete() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..3) {
            println("Inserting $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("    $it, ") }
        }
        for (i in 1..3) {
            println("Deleting ${4-i}")
            tree.remove(TXIdentifier(), DoublePageEntry((4-i).toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("      $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestDeleteOneSplit() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..5) {
            println("Inserting $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("    $it, ") }
        }
        for (i in 1..5) {
            println("Deleting $i")
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("    $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestReverseDeleteOneSplit() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..5) {
            println("Inserting $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("    $it, ") }
        }
        for (i in 1..5) {
            println("Deleting ${6-i}")
            tree.remove(TXIdentifier(), DoublePageEntry((6-i).toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("    $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestDeleteWithRootSplit() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..20) {
            println("During Insert $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        }
        println("After Insert")
            tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        for (i in 1..20) {
            println("During Remove $i")
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestReverseDeleteWithRootSplit() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..20) {
            println("During Insert $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        }
        println("After Insert")
        tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        for (i in 1..20) {
            println("During Remove ${21-i}")
            tree.remove(TXIdentifier(), DoublePageEntry((21 - i).toDouble()), value)
            tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestShuffleDeleteWithRootSplit() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..180) {
            println("During Insert $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
        println("After Insert")
        tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        val numberList = (1..180).toList().shuffled(Random(11))
        for (i in numberList) {
            println("During Remove $i")
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
    }
}