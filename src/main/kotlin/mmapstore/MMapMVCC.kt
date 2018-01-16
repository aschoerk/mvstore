package mmapstore

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock



class TransactionInfo() {

    class FileChangeInfo(val file: IMMapPageFile) {
        val changedPages = mutableMapOf<Int,Int>()
        val freedPages = mutableSetOf<Int>()

        fun pageAlreadyChanged(pageNo: Int) : Boolean {
            return changedPages?.containsKey(pageNo) ?: false
        }

        fun mapPage(originalNo: Int, copyNo: Int) {
            assert (!changedPages.containsKey(originalNo))
            changedPages[originalNo] = copyNo
        }

        fun getMappedPage(pageNo: Int) : Int? {
            assert (!freedPages.contains(pageNo))
            return changedPages[pageNo]
        }

        fun freePage(pageNo: Int) {
            assert (!freedPages.contains(pageNo))
            freedPages.add(pageNo)
        }

    }

    fun getFileChangeInfo(fileId: IMMapPageFile): FileChangeInfo {
        var fci = fileChangeInfo[fileId]
        if (fci == null) {
            fci = FileChangeInfo(fileId)
            fileChangeInfo[fileId] = fci
        }
        return fci
    }

    val id = (MVCC.transactionIds.incrementAndGet() and 0xFFFF).toShort()
    val baseId = MVCC.nextTra(id)

    val fileChangeInfo = mutableMapOf<IMMapPageFile, FileChangeInfo>()

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
    class TraIdPageNo(val traId: Long, val pageNo: Int, val currentMaxTraId: Long)
    var startTime = java.lang.System.currentTimeMillis()
    val changes = AtomicLong(0)
    val transactionIds = AtomicInteger(0)
    val preImagesPerFile = mutableMapOf<IMMapPageFile,MutableMap<Int, MutableList<TraIdPageNo>>>()

    var threadLocalTransaction: ThreadLocal<TransactionInfo> = ThreadLocal()

    fun clearCurrentTransaction() {
        assert(getCurrentTransaction() != null)
        assert(threadsForTransactions.containsKey(getCurrentTransaction()))
        threadsForTransactions.remove(getCurrentTransaction())
        threadLocalTransaction.set(null)
    }

    fun setCurrentTransaction(tra: TransactionInfo) {
        synchronized(this) {
            assert(!threadsForTransactions.containsKey(tra))
            threadLocalTransaction.set(tra)
            threadsForTransactions[tra] = Thread.currentThread().id
        }
    }

    fun getCurrentTransaction(): TransactionInfo? {
        return threadLocalTransaction.get()
    }

    val transactions = mutableMapOf<Short,TransactionInfo>()
    val threadsForTransactions = mutableMapOf<TransactionInfo, Long>()

    var lastTraId = 0L

    fun nextTra(traId: Short) : Long {
        lastTraId = changes.incrementAndGet() shl 16 or traId.toLong()
        return lastTraId
    }

    fun nextTra(traId: Long) : Long {
        lastTraId = changes.incrementAndGet() shl 16 or (traId and 0xFFFF).toLong()
        return lastTraId
    }

    fun getPreImage(file: IMMapPageFile, pageNo: Int, traId: Long) : Int? {
        return preImagesPerFile[file]?.get(pageNo)?.find({ it.traId == traId })?.pageNo ?: null
    }

    fun getMinimalPreImage(file: IMMapPageFile, pageNo: Int, traId: Long) : Int? {
        val preImagesOfFile = getPreImagesOfFile(file)
        val preImages = preImagesOfFile[pageNo]
        if (preImages != null) {
            return preImages.filter { it.traId <= traId }?.maxBy { it.traId }?.pageNo
        }
        return null
    }

    private fun getPreImagesOfFile(file: IMMapPageFile): MutableMap<Int, MutableList<TraIdPageNo>> {
        if (!preImagesPerFile.containsKey(file)) {
            synchronized(preImagesPerFile) {
                preImagesPerFile[file] = mutableMapOf<Int, MutableList<TraIdPageNo>>()
            }
        }
        return preImagesPerFile[file]!!
    }

    fun addPreImage(file: IMMapPageFile, pageNo: Int, traId: Long, preImageNo: Int) {
        val preImagesOfFile = getPreImagesOfFile(file)
        synchronized(preImagesOfFile) {
            var existing = preImagesOfFile[pageNo]
            if (existing == null) {
                existing = mutableListOf()
                preImagesOfFile[pageNo] = existing
            }
            existing.add(TraIdPageNo(traId, preImageNo, lastTraId))
        }
    }


    // everything done after this must be
    // - atomar: so others can only see all changes or no changes (so rollback must be possible)
    // - consistent
    // - isolated:
    // - durable
    fun begin(): TransactionInfo {
        if (getCurrentTransaction() != null) {
            throw IllegalStateException("Transaction running")
        }

        val transactionInfo = TransactionInfo()
        setCurrentTransaction(transactionInfo)
        transactions[transactionInfo.id] = transactionInfo
        return transactionInfo
    }

    fun commit() {
        val transactionInfo = getCurrentTransaction()
        if (transactionInfo == null) {
            throw IllegalStateException("No Transaction running")
        }
        try {
            transactionInfo.fileChangeInfo.entries.forEach({
                val file = it.key
                file.lock.lock()
                val traId = nextTra(transactionInfo.baseId)
                try {
                    it.value.changedPages.entries.forEach({
                        if (it.key != it.value) {
                            val original = file.getPage(it.key)
                            val orgId = file.getLong(original.offset + CHANGED_BY_TRA_INDEX)

                            // at the moment: can only handle not overlapping changes
                            assert(orgId <= transactionInfo.baseId)
                            if (MVCC.getPreImage(file, it.key, orgId) == null) {
                                val preImage = file.newPage()
                                file.copy(original.offset, preImage.offset, PAGESIZE.toInt())
                                MVCC.addPreImage(file, it.key, orgId, preImage.number)
                            }
                            val copy = file.getPage(it.value)
                            file.copy(copy.offset, original.offset, PAGESIZE.toInt())
                            file.setLong(copy.offset + CHANGED_BY_TRA_INDEX, traId)
                            file.freePage(copy)
                        }
                    })
                } finally {
                    file.lock.unlock()
                }
            })
        } finally {
            clearCurrentTransaction()
            cleanup(transactionInfo!!)
        }
    }

    private fun cleanup(transactionInfo: TransactionInfo) {
        // transactionInfo must be in transactions
        if (transactionInfo.id <= transactions.keys.min()!!) {
            preImagesPerFile.entries.forEach({
                val file = it.key
                file.lock.lock()
                try {
                    it.value.entries.forEach({
                        val orgPage = it.key
                        val preImages = it.value
                        preImages.filter { it.currentMaxTraId <= transactionInfo.baseId }.forEach({
                            file.freePage(it.pageNo)
                            preImages.remove(it)
                        })
                    })

                } finally {
                    it.key.lock.unlock()
                }
            })
        }
    }

    fun rollback() {
        val transactionInfo = getCurrentTransaction()
        if (transactionInfo == null) {
            throw IllegalStateException("No Transaction running")
        }

        try {
            transactionInfo.fileChangeInfo.entries.forEach({
                val file = it.key
                it.value.changedPages.values.forEach({
                    file.freePage(file.getPage(it))
                })
            })
        } finally {
            clearCurrentTransaction()
            cleanup(transactionInfo)
        }
    }

    fun switchTra(tra: TransactionInfo) {

    }


    // changes were temporary, now they should be written to the final destination. Before that, the original data must be
    // kept for other transactions not yet allowed  to see them.
    fun savePoint() {


    }

    operator fun get(traId: Long) = transactions[(traId and 0xFFFF).toShort()]

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
        MVCC.getCurrentTransaction()?.btreeOperations?.add(BTreeOperation(btree, BTreeOperationTypes.INSERT, key, value))
        btree.insert(key, value)
    }

    override fun remove(key: ComparableMMapPageEntry, value: MMapPageEntry) {
        MVCC.getCurrentTransaction()?.btreeOperations?.add(BTreeOperation(btree, BTreeOperationTypes.DELETE, key, value))
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


class MVCCFile(val file: IMMapPageFile) : IMMapPageFile {
    override val lock: ReentrantLock
        get() = file.lock

    override fun getPage(page: Int): MMapPageFilePage = MMapPageFilePage(this, convertOffset(page * PAGESIZE))

    override val fileId: FileId
        get() = file.fileId


    fun copyPageIfNecessary(idx: Long) {
        val pageNo = (idx / PAGESIZE).toInt()
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            lock.lock()
            try {
                if (!transactionInfo.getFileChangeInfo(this).pageAlreadyChanged(pageNo)) {
                    val newPage = file.newPage()
                    file.copy(convertOffset(pageNo * PAGESIZE), newPage.offset, PAGESIZE.toInt())
                    transactionInfo.getFileChangeInfo(this).mapPage(pageNo, newPage.number)
                    file.setLong(newPage.offset + CHANGED_BY_TRA_INDEX, transactionInfo.baseId)
                }
            } finally {
                lock.unlock()
            }
        }
    }

    override fun getByte(idx: Long) = file.getByte(idx)

    private fun convertOffset(idx: Long): Long {
        var pageNo = (idx / PAGESIZE).toInt()

        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            var lastTra = transactionInfo.baseId
            var maxChange = Long.MAX_VALUE
            val pno = transactionInfo.getFileChangeInfo(this).changedPages[pageNo]
            if (pno != null) {
                return pno * PAGESIZE + idx % PAGESIZE
            } else {
                val pno = MVCC.getMinimalPreImage(file, pageNo, transactionInfo.baseId)
                if (pno != null)
                    return pno * PAGESIZE + idx % PAGESIZE
                else
                    return idx
            }
        }
        return idx
    }

    override fun setByte(idx: Long, i: Byte) {
        copyPageIfNecessary(idx)
        file.setByte(convertOffset(idx), i)
    }

    override fun getChar(idx: Long) = file.getChar(idx)
    override fun setChar(idx: Long, c: Char) {
        copyPageIfNecessary(idx)
        file.setChar(convertOffset(idx), c)
    }

    override fun getShort(idx: Long) = file.getShort(convertOffset(idx))
    override fun setShort(idx: Long, i: Short) {
        copyPageIfNecessary(idx)
        file.setShort(convertOffset(idx), i)
    }

    override fun getInt(idx: Long) = file.getInt(convertOffset(idx))
    override fun setInt(idx: Long, i: Int) {
        copyPageIfNecessary(idx)
        file.setInt(convertOffset(idx), i)
    }

    override fun getLong(idx: Long) = file.getLong(convertOffset(idx))
    override fun setLong(idx: Long, i: Long) {
        copyPageIfNecessary(idx)
        file.setLong(convertOffset(idx), i)
    }

    override fun getFloat(idx: Long) = file.getFloat(convertOffset(idx))
    override fun setFloat(idx: Long, f: Float) {
        copyPageIfNecessary(idx)
        file.setFloat(convertOffset(idx), f)
    }

    override fun getDouble(idx: Long) = file.getDouble(convertOffset(idx))
    override fun setDouble(idx: Long, f: Double) {
        copyPageIfNecessary(idx)
        file.setDouble(convertOffset(idx), f)
    }

    override fun getBoolean(idx: Long): Boolean = getByte(idx) != 0.toByte()
    override fun setBoolean(idx: Long, b: Boolean) = setByte(idx, (if (b) 1 else 0).toByte())

    override fun getByteArray(idx: Long, ba: ByteArray) = file.getByteArray(idx, ba)
    override fun setByteArray(idx: Long, ba: ByteArray) {
        copyPageIfNecessary(idx)
        file.setByteArray(convertOffset(idx), ba)
    }

    override fun copy(from: Long, to: Long, size: Int) {
        file.copy(convertOffset(from), convertOffset(to), size)
    }

    override fun newPage(): MMapPageFilePage {
        var result = getPage(file.newPage().number)
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            file.setLong(result.offset + CHANGED_BY_TRA_INDEX, transactionInfo.baseId)
            transactionInfo.getFileChangeInfo(this).mapPage(result.number, result.number)
        }
        return result
    }

    override fun freePage(page: MMapPageFilePage) {
        freePage(page.number)
    }

    override fun freePage(page: Int) {
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            if ( file.getLong(page * PAGESIZE + CHANGED_BY_TRA_INDEX) == transactionInfo.baseId) {
                // page was allocated for and during this transaction
                file.freePage(page)
            }
            transactionInfo.getFileChangeInfo(this).freePage(page)
        } else {
            file.freePage(page)
        }
    }


    override fun isUsed(pageNum: Int): Boolean {
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            if (transactionInfo.getFileChangeInfo(this).freedPages.contains(pageNum))
                return false
        }
        return file.isUsed(pageNum)
    }

    override fun isFree(pageNum: Int): Boolean {
        return !isUsed(pageNum)
    }

}