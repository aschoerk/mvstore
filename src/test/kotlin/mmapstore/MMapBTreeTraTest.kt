package mmapstore

import org.junit.Test
import kotlin.test.assertEquals

/**
 * @author aschoerk
 */
class MMapBTreeTraTest : MMapBTreeTestBase() {

    @Test
    fun simpleBTreeTestWithValueWithoutSplit() {
        val tree = file!!.createBTree("test", true)
        tree.doCheck = true
        val tra1 = MVCC.begin()

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..3) {
            println("Inserting $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        }
        MVCC.commit()
        val tra2 = MVCC.begin()
        for (i in 1..3) {
            println("Deleting $i")
            tree.remove(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("      $it, ") }
        }
        MVCC.commit()
    }

    @Test
    fun testByte() {
        testPEs(BytePageEntry(1), BytePageEntry(2))
    }

    @Test
    fun testBoolean() {
        testPEs(BooleanPageEntry(true), BooleanPageEntry(true))
    }

    @Test
    fun testShort() {
        testPEs(ShortPageEntry(1), ShortPageEntry(2))
    }

    @Test
    fun testInt() {
        testPEs(IntPageEntry(1), IntPageEntry(2))
    }

    @Test
    fun testLong() {
        testPEs(LongPageEntry(1), LongPageEntry(2))
    }

    @Test
    fun testFloat() {
        testPEs(FloatPageEntry(1.0f), FloatPageEntry(2.0f))
    }

    @Test
    fun testDouble() {
        testPEs(DoublePageEntry(1.0), DoublePageEntry(2.0))
    }

    @Test
    fun testString() {
        testPEs(StringPageEntry("1.0"), StringPageEntry("2.0"))
    }

    @Test
    fun testByteArray() {
        testPEs(ByteArrayPageEntry("1.0".toByteArray()), ByteArrayPageEntry("2.0".toByteArray()))
    }

    private fun testPEs(a: MMapPageEntry, b: MMapPageEntry, level: Int = 0) {
        if (level > 3)
            return
        val tree = file!!.createBTree("test$level") as IMMapBTree
        val testPage = tree.file.newPage()
        testPage.add(a)
        MVCC.begin()
        testPage.add(b)
        val check = {
            assertEquals(2,testPage.countEntries())
            testPage.entries().forEach {
                val res = unmarshalFrom(testPage, it)
                assert(res.type == a.type || res.type == b.type)
                assert(res == a || res == b)
            }
        }
        check()
        MVCC.commit()
        check()
        MVCC.begin()
        testPage.add(b)
        assertEquals(3,testPage.countEntries())
        MVCC.rollback()
        check()
        val lpe = ListPageEntry(a, b)
        testPEs(lpe, b, level + 1)
    }

    @Test
    fun simpleBTreeTestWithValueWithSplit() {
        val tree = file!!.createBTree("test")
        tree.doCheck = true
        var otherTra = MVCC.begin()
        val tra1 = MVCC.begin()

        val value = ByteArrayPageEntry(ByteArray(2000))  // 4 Entries per page possible
        // split will be necessary
        for (i in 1..7) {
            println("Inserting $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("    $it, ") }
        }
        MVCC.commit()
        MVCC.setCurrentTransaction(otherTra)
        assert(!tree.iterator().hasNext())
        MVCC.commit()
        otherTra = MVCC.begin()
        val tra2 = MVCC.begin()
        for (i in 1..7) {
            println("Deleting $i")
            tree.remove(DoublePageEntry(i.toDouble()), value)
            tree.iterator().forEach { println("      $it, ") }
        }
        assert(!tree.iterator().hasNext())
        MVCC.commit()
        MVCC.setCurrentTransaction(otherTra)
        assert(tree.iterator().hasNext())
        MVCC.commit()
        otherTra = MVCC.begin()
        assert(!tree.iterator().hasNext())
    }

    @Test
    fun simple100BTreeNoTraTest() {
        var tra = MVCC.begin()
        val tree = file!!.createBTree("test", true)
        val value = StringPageEntry("Long string to be added as Value into tree but is it long enough??")
        val valNumber = 70
        for (i in 1..valNumber) {
            println("run: $i")
            tree.insert(DoublePageEntry(i.toDouble()), value)
            (1..i).forEach { println("$it: ${tree.find(DoublePageEntry(it.toDouble()))}")}
        }

        //(1..valNumber).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) != null)}
        (1..valNumber).forEach {
            assert(tree.find(DoublePageEntry(it.toDouble())) != null)
            tree.remove(DoublePageEntry(it.toDouble()), value)
        }
        //(1..valNumber).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) == null)}
        MVCC.commit()
        (1..valNumber).forEach {
            assert(tree.find(DoublePageEntry(it.toDouble())) == null)
        }
    }
    @Test
    fun simple100BTreeTraTest() {
        val tree = file!!.createBTree("test", true)
        tree.doCheck = true
        val tra1 = MVCC.begin()
        val tra2 = MVCC.begin()
        val value = StringPageEntry("Long string to be added as Value into tree but is it long enough??")
        for (i in 1..100) {
            tree.insert(DoublePageEntry(i.toDouble()), value)
        }
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) != null)}
        MVCC.setCurrentTransaction(tra1)
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) == null)}
        MVCC.setCurrentTransaction(tra2)
        MVCC.commit()
        // not seen in tra1 yet in spite of commit
        MVCC.setCurrentTransaction(tra1)
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) == null)}
        // can be seen in new transaction
        val tra3 = MVCC.begin()
        val tra4 = MVCC.begin()
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) != null)}
        (1..100).forEach {
            assert(tree.find(DoublePageEntry(it.toDouble())) != null)
            tree.remove(DoublePageEntry(it.toDouble()), value)
        }
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) == null)}
        // not deleted in tra4
        MVCC.setCurrentTransaction(tra3)
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) != null)}
        // now commit deletes
        MVCC.setCurrentTransaction(tra4)
        MVCC.commit()
        // yet nothing seen in tra1
        MVCC.setCurrentTransaction(tra1)
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) == null)}
        MVCC.commit()
        // deletes from tra4 not seen in tra3
        MVCC.setCurrentTransaction(tra3)
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) != null)}
        MVCC.commit()
        // tra5 will see the deletes from tra3
        val tra5 = MVCC.begin()
        (1..100).forEach{assert(tree.find(DoublePageEntry(it.toDouble())) == null)}
        MVCC.commit()
    }

    fun canRollbackAfterSplit() {

    }

    fun canHandleTransactionsInMultipleTrees() {

    }


    fun canHandleTransactionsInMultipleFiles() {

    }

    fun canHandleTransactionsInMultipleThreads() {

    }

}