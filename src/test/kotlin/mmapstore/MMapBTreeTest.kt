package mmapstore


import org.agrona.concurrent.MappedResizeableBuffer
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.RandomAccessFile
import java.util.*

/**
 * @author aschoerk
 */

open class MMapBTreeTestBase {
    protected var randomAccessFile: RandomAccessFile? = null

    protected var file: MMapDbFile? = null

    @Before fun setupMMapBtreeTestBase() {
        File("/tmp/testfile.bin").delete()
        MVCC.init()

        val f = RandomAccessFile("/tmp/testfile.bin", "rw")
        f.seek(100000 * 8192 - 1)
        f.writeByte(0xFF)
        val b = MappedResizeableBuffer(f.channel,0L,f.length() )
        this.file = MMapDbFile(MMapPageFile(b, f.length()))
        this.randomAccessFile = f
    }

}

class MMapBTreeTest : MMapBTreeTestBase() {


    @Test
    fun simpleBTreeTest() {
        val tree = file!!.createBTree("test")
        val value = EmptyPageEntry()
        for (i in 1..100) {
            tree.insert(DoublePageEntry(i.toDouble()), value)
        }
        for (i in 1..100) {
            tree.remove(DoublePageEntry(i.toDouble()), value)
        }
    }

    @Test
    fun canStoreDuplicateKeysWithEqualValues() {
        val tree = file!!.createBTree("test")
        for (i in 1..100) {
            tree.insert(DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
            tree.insert(DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
        for (i in 1..100) {
            tree.remove(DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
        for (i in 1..100) {
            tree.remove(DoublePageEntry(i.toDouble()), DoublePageEntry(i.toDouble()))
        }
    }

    @Test
    fun canStoreDuplicateKeysWithManyDifferentValues() {
        val tree = file!!.createBTree("test")

        val valueNumber = 100000
        (1..valueNumber).shuffled().forEach( {
            tree.insert(DoublePageEntry(it.toDouble()), DoublePageEntry((it+1).toDouble()))
            tree.insert(DoublePageEntry(it.toDouble()), DoublePageEntry(it.toDouble()))
            if (it % 1000 == 0) {
                println("Inserted $it pairs")
                checkAndOutput(tree)
                val valuesbetween = (1..it / 100 ).shuffled()
                valuesbetween.forEach( {
                    tree.insert(DoublePageEntry(it+0.5), DoublePageEntry((it+1).toDouble()))
                })
                valuesbetween.forEach( {
                    tree.remove(DoublePageEntry(it+0.5), DoublePageEntry((it+1).toDouble()))
                })
                checkAndOutput(tree)
            }

        })
        (1..valueNumber).shuffled().forEach( {
            tree.remove(DoublePageEntry(it.toDouble()), DoublePageEntry((it).toDouble()))
            if (it % 1000 == 0) {
                println("Removed $it second values")
                checkAndOutput(tree)
            }
        })
        (1..valueNumber).shuffled().forEach( {
            tree.remove(DoublePageEntry(it.toDouble()), DoublePageEntry((it+1).toDouble()))
            if (it % 1000 == 0) {
                println("Removed $it second values")
                checkAndOutput(tree)
            }
        })
    }

    @Test
    fun simpleBTreeTestWithValue() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..3) {
            println("Inserting $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        }
        for (i in 1..3) {
            println("Deleting $i")
            tree.remove(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("      $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestReverseDelete() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..3) {
            println("Inserting $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        }
        for (i in 1..3) {
            println("Deleting ${4-i}")
            tree.remove(DoublePageEntry((4-i).toDouble()), value)
            tree.iterator().forEach { println("      $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestDeleteOneSplit() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        (1..5).forEach( {
            println("Inserting $it")
            tree.insert(DoublePageEntry(it.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        })
        for (i in 1..5) {
            println("Deleting $i")
            tree.remove(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestReverseDeleteOneSplit() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..5) {
            println("Inserting $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        }
        (1..5).reversed().forEach({
            println("Deleting $it")
            tree.remove(DoublePageEntry(it.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        })
    }

    @Test
    fun simpleBTreeTestDeleteWithRootSplit() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..20) {
            println("During Insert $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("     $it, ") }
        }
        println("After Insert")
            tree.iterator().forEach { println("     $it, ") }
        for (i in 1..20) {
            println("During Remove $i")
            tree.remove(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("     $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestReverseDeleteWithRootSplit() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..20) {
            println("During Insert $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("     $it, ") }
        }
        println("After Insert")
        tree.iterator().forEach { println("     $it, ") }
        for (i in 1..20) {
            println("During Remove ${21-i}")
            tree.remove(DoublePageEntry((21 - i).toDouble()), value)
            tree.iterator().forEach { println("     $it, ") }
        }
    }

    @Test
    fun simpleBTreeTestShuffleDeleteWithRootSplit() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        val numberToInsert = 180
        for (i in 1..180) {
            println("During Insert $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
        }
        println("After Insert")
        tree.iterator().forEach { println("     $it, ") }
        val numberList = (1..180).toList().shuffled(Random(11))
        for (i in numberList) {
            println("During Remove $i")
            tree.remove(DoublePageEntry(i.toDouble()), value)
        }
    }

    @Test
    fun simpleBTreeTestShuffleDeleteAndInserts() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        val numberToInsert = 180
        val numberList1 = (1..numberToInsert).toList().shuffled(Random(11))

        for (i in numberList1) {
            println("During Insert $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
        }
        println("After Insert")
        tree.iterator().forEach { println("     $it, ") }
        (1..numberToInsert).shuffled(Random(11)).forEach( {
            println("During Remove $it")
            tree.remove(DoublePageEntry(it.toDouble()), value)
        })
    }

    @Test
    fun simpleBTreeTestShuffleDeleteAndInsertsSinglePageRecords() {
        val tree = file!!.createBTree("test")

        val value = ByteArrayPageEntry(ByteArray(5000))  // 4 Entries per page possible
        // split will be necessary
        val numberToInsert = 180
        val numberList1 = (1..numberToInsert).toList().shuffled(Random(11))

        for (i in numberList1) {
            println("During Insert $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
        }
        println("After Insert")
        tree.iterator().forEach { println("     $it, ") }
        (1..numberToInsert).shuffled(Random(11)).forEach( {
            println("During Remove $it")
            tree.remove(DoublePageEntry(it.toDouble()), value)
        })
    }

    @Test
    fun canShuffledDeletesAndReInsertsAndSplitDuringDeletes() {
        val tree = file!!.createBTree("test")

         // split will be necessary
        val numberToInsert = 250

        (1..numberToInsert).shuffled(Random(200)).forEach(
                {
                    tree.insert(DoublePageEntry(it.toDouble()), byteArrayPageEntry(it))
                    println(tree.check())

                }
        )
        println("After Insert \n ${tree.check()}")
        tree.iterator().forEach { println("     $it, ") }
        val loops = 30
        (1..loops).forEach({ loopcount ->
            var half = numberToInsert / 2
            println("During Remove first Half $loopcount")
            (1..half).shuffled(Random(loopcount % 100L)).forEach(
                    {
                        print(" $it")
                        tree.remove( DoublePageEntry(it.toDouble()), byteArrayPageEntry(it))
                        checkAndOutput(tree)
                    }
            )
            println("During Reinsert first Half $loopcount")
            (1..half).toList().shuffled(Random(loopcount % 100L)).forEach(
                    {
                        print(" $it")
                        tree.insert(DoublePageEntry(it.toDouble()), byteArrayPageEntry(it))
                        checkAndOutput(tree)
                    }
            )
            println("During Remove second Half $loopcount")
            (half+1..numberToInsert).shuffled(Random(loopcount % 100L)).forEach(
                    {
                        print(" $it")
                        tree.remove(DoublePageEntry(it.toDouble()), byteArrayPageEntry(it))
                        checkAndOutput(tree)
                    }
            )
            println("During Reinsert second Half $loopcount")
            (half+1..numberToInsert).shuffled(Random(loopcount % 100L)).forEach(
                    {
                        print(" $it")
                        tree.insert(DoublePageEntry(it.toDouble()), byteArrayPageEntry(it))
                        checkAndOutput(tree)
                    }
            )

        })
        println("During complete Remove at end")
        (1..numberToInsert).shuffled(Random(200)).forEach(
                {
                    print(" $it")
                    tree.remove(DoublePageEntry(it.toDouble()), byteArrayPageEntry(it))
                    checkAndOutput(tree)
                }
        )
    }

    fun checkAndOutput(tree: IMMapBTree) {
        val res = tree.check()
        if (res.length > 0)
            println("\n$res")
    }

    private fun byteArrayPageEntry(it: Int) = ByteArrayPageEntry(ByteArray(3 * (it % 100) * 10 + 2000))
}