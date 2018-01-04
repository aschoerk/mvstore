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
        val numberToInsert = 180
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

    @Test
    fun simpleBTreeTestShuffleDeleteAndInserrts() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        val numberToInsert = 180
        val numberList1 = (1..numberToInsert).toList().shuffled(Random(11))

        for (i in numberList1) {
            println("During Insert $i")
            tree.insert(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
        println("After Insert")
        tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        val numberList2 = (1..numberToInsert).toList().shuffled(Random(11))
        for (i in numberList2) {
            println("During Remove $i")
            tree.remove(TXIdentifier(), DoublePageEntry(i.toDouble()), value)
        }
    }

    @Test
    fun simpleBTreeTestShuffleDeleteAndReInserts() {
        val filep = file ?: throw AssertionError("")
        val tree = NioBTree(filep)
        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        val numberToInsert = 250
        val numberList1 = (1..numberToInsert).toList().shuffled(Random(11))

        (1..numberToInsert).shuffled(Random(200)).forEach(
                {
                    tree.insert(TXIdentifier(), DoublePageEntry(it.toDouble()), ByteArrayPageEntry(ByteArray((it % 100) * 20)))

                }
        )
        println("After Insert")
        tree.iterator(TXIdentifier()).forEach { println("     $it, ") }
        (1..1000).forEach({ loopcount ->
            var half = numberToInsert / 2
            println("During Remove first Half $loopcount")
            (1..half).shuffled(Random(loopcount % 100L)).forEach(
                    {

                        tree.remove(TXIdentifier(), DoublePageEntry(it.toDouble()), ByteArrayPageEntry(ByteArray((it % 100) * 20)))

                    }
            )
            println("During Reinsert first Half $loopcount")
            (1..half).toList().shuffled(Random(loopcount % 100L)).forEach(
                    {
                        tree.insert(TXIdentifier(), DoublePageEntry(it.toDouble()), ByteArrayPageEntry(ByteArray((it % 100) * 20)))

                    }
            )
            println("During Remove second Half $loopcount")
            (half+1..numberToInsert).shuffled(Random(loopcount % 100L)).forEach(
                    {
                        tree.remove(TXIdentifier(), DoublePageEntry(it.toDouble()), ByteArrayPageEntry(ByteArray((it % 100) * 20)))

                    }
            )
            println("During Reinsert second Half $loopcount")
            (half+1..numberToInsert).shuffled(Random(loopcount % 100L)).forEach(
                    {
                        tree.insert(TXIdentifier(), DoublePageEntry(it.toDouble()), ByteArrayPageEntry(ByteArray((it % 100) * 20)))
                    }
            )

        })
        println("During complete Remove at end")
        (1..numberToInsert).shuffled(Random(200)).forEach(
                {
                    tree.remove(TXIdentifier(), DoublePageEntry(it.toDouble()), ByteArrayPageEntry(ByteArray((it % 100) * 20)))

                }
        )
    }
}