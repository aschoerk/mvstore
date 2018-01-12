package mmapstore

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock


class TransactionInfo() {
    val id = (MVCC.transactionIds.incrementAndGet() and 0xFFFF).toShort()
    val baseId = MVCC.nextTra(id)

    val undoMap = mutableMapOf<Int,Int>()
    val freedPages = mutableSetOf<Int>()
    val btreeOperations = mutableListOf<BTreeOperation>()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TransactionInfo

        if (baseId != other.baseId) return false

        return true
    }

    override fun hashCode(): Int {
        return baseId.toInt()
    }


}


object MVCC {
    var startTime = java.lang.System.currentTimeMillis()
    val changes = AtomicLong(0)
    val transactionIds = AtomicInteger(0)

    var threadLocal: ThreadLocal<TransactionInfo> = ThreadLocal()
    val transactions = mutableMapOf<Short,TransactionInfo>()

    fun nextTra(traId: Short) = changes.incrementAndGet() shl 16 or traId.toLong()


    // everything done after this must be
    // - atomar: so others can only see all changes or no changes (so rollback must be possible)
    // - consistent
    // - isolated:
    // - durable
    fun begin() {
        if (threadLocal.get() != null) {
            throw IllegalStateException("Transaction running")
        }

        val transactionInfo = TransactionInfo()
        threadLocal.set(transactionInfo)
        transactions[transactionInfo.id] = transactionInfo
    }

    fun commit() {
        if (threadLocal.get() == null) {
            throw IllegalStateException("No Transaction running")
        }
        threadLocal.set(null)
    }

    fun rollback() {

    }


    // changes were temporary, now they should be written to the final destination. Before that, the original data must be
    // kept for other transactions not yet allowed  to see them.
    fun savePoint() {


    }

    fun current() = threadLocal.get()

    operator fun get(traId: Long) = transactions[traId]

}

enum class BTreeOperationTypes { INSERT, DELETE }

class BTreeOperation(val btree: IMMapBTree,
                     val op: BTreeOperationTypes,
                     val key: ComparableMMapPageEntry,
                     val value: MMapPageEntry)

class MVCCBTree(val btree: IMMapBTree) : IMMapBTree {
    override var doCheck: Boolean
        get() = btree.doCheck
        set(value) {btree.doCheck = value}

    override fun insert(key: ComparableMMapPageEntry, value: MMapPageEntry) {
        MVCC.current().btreeOperations.add(BTreeOperation(btree, BTreeOperationTypes.INSERT, key, value))
        btree.insert(key, value)
    }

    override fun remove(key: ComparableMMapPageEntry, value: MMapPageEntry) {
        MVCC.current().btreeOperations.add(BTreeOperation(btree, BTreeOperationTypes.DELETE, key, value))
        btree.remove(key, value)
    }

    override fun iterator(): Iterator<MMapPageEntry> {
        return btree.iterator()
    }

    override fun find(key: ComparableMMapPageEntry): List<MMapPageEntry>? {
        return btree.find(key)
    }

    override fun findSingle(key: ComparableMMapPageEntry): MMapPageEntry? {
        return btree.findSingle(key)
    }

    override fun check(): String {
        return btree.check()
    }
}


class MVCCFile(val file: MMapPageFile) : IMMapPageFile {

    val b = file

    val pageCopyLock = ReentrantLock()


    fun copyPageIfNecessary(idx: Long) {
        val pageNo = (idx / PAGESIZE).toInt()
        val transactionInfo = MVCC.current()
        if (transactionInfo != null) {
            pageCopyLock.lock()
            try {
                if (!transactionInfo.undoMap.containsKey(pageNo)) {
                    val newPage = file.newPage()
                    file.move(pageNo * PAGESIZE, newPage.offset, PAGESIZE.toInt())
                    transactionInfo.undoMap[pageNo] = newPage.number
                    file.setLong(pageNo * PAGESIZE + CHANGED_BY_TRA_INDEX, transactionInfo.traNum)
                }
            } finally {
                pageCopyLock.unlock()
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

    override fun newPage(): MMapPageFilePage {
        return file.newPage()
    }

    override fun freePage(page: MMapPageFilePage) {
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