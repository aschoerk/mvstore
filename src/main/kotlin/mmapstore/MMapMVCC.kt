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

        fun unmapPage(originalNo: Int) {
            assert (changedPages.containsKey(originalNo))
            changedPages.remove(originalNo)
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
    val transactions = mutableMapOf<Short,TransactionInfo>()
    val threadsForTransactions = mutableMapOf<Short, Long>()

    var lastTraId = 0L

    fun clearCurrentTransaction() {
        assert(getCurrentTransaction() != null)
        val traId = getCurrentTransaction()!!.id
        assert(transactions.contains(traId))
        assert(threadsForTransactions.containsKey(traId))
        threadsForTransactions.remove(traId)
        threadLocalTransaction.set(null)
    }

    fun setCurrentTransaction(tra: TransactionInfo) {
        synchronized(this) {
            assert(transactions.contains(tra.id))
            assert(!threadsForTransactions.containsKey(tra.id))
            if (threadLocalTransaction.get() != null) {
                assert(threadsForTransactions.containsKey(threadLocalTransaction.get().id))
                threadsForTransactions.remove(threadLocalTransaction.get().id)
            }
            threadLocalTransaction.set(tra)
            threadsForTransactions[tra.id] = Thread.currentThread().id
        }
    }

    fun getCurrentTransaction(): TransactionInfo? {
        return threadLocalTransaction.get()
    }


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
        assert (!file.getPage(pageNo).preImage)
        assert (!file.getPage(pageNo).traPage)
        file.getPage(pageNo).preImage = true
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

        val transactionInfo = TransactionInfo()
        transactions[transactionInfo.id] = transactionInfo
        setCurrentTransaction(transactionInfo)
        return transactionInfo
    }

    fun commit() {
        val transactionInfo = getCurrentTransaction()
        if (transactionInfo == null) {
            throw IllegalStateException("No Transaction running")
        }
        clearCurrentTransaction()
        try {
            transactionInfo.fileChangeInfo.entries.forEach({
                val wrappedFile = it.key.wrappedFile
                wrappedFile.lock.lock()
                val traId = nextTra(transactionInfo.baseId)
                try {
                    it.value.changedPages.entries.forEach({
                        if (it.key != it.value) {
                            val original = wrappedFile.getPage(it.key)
                            val orgId = wrappedFile.getLong(original.offset + CHANGED_BY_TRA_INDEX)

                            // at the moment: can only handle not overlapping changes
                            assert(orgId <= transactionInfo.baseId)
                            if (MVCC.getPreImage(wrappedFile, it.key, orgId) == null) {
                                val preImage = wrappedFile.newPage()
                                wrappedFile.copy(original.offset, preImage.offset, PAGESIZE.toInt())
                                MVCC.addPreImage(wrappedFile, it.key, orgId, preImage.number)
                            }
                            val copy = wrappedFile.getPage(it.value)
                            wrappedFile.copy(copy.offset, original.offset, PAGESIZE.toInt())
                            wrappedFile.setLong(copy.offset + CHANGED_BY_TRA_INDEX, traId)
                            wrappedFile.freePage(copy)
                        }
                    })
                } finally {
                    wrappedFile.lock.unlock()
                }
            })
        } finally {

            cleanup(transactionInfo!!)
        }
    }

    private fun cleanup(transactionInfo: TransactionInfo) {
        // transactionInfo must be in transactions
        if (transactionInfo.id <= transactions.keys.min()!!) {
            preImagesPerFile.entries.forEach({
                val wrappedFile = it.key.wrappedFile
                wrappedFile.lock.lock()
                try {
                    it.value.entries.forEach({
                        val orgPage = it.key
                        val preImages = it.value
                        preImages.filter { it.currentMaxTraId <= transactionInfo.baseId }.forEach({
                            assert(wrappedFile.getPage(it.pageNo).preImage)
                            wrappedFile.freePage(it.pageNo)
                            preImages.remove(it)
                        })
                    })

                } finally {
                    it.key.lock.unlock()
                }
            })
        }
        transactions.remove(transactionInfo.id)
    }

    fun rollback() {
        val transactionInfo = getCurrentTransaction()
        if (transactionInfo == null) {
            throw IllegalStateException("No Transaction running")
        }

        try {
            transactionInfo.fileChangeInfo.entries.forEach({
                val wrappedFile = it.key.wrappedFile
                it.value.changedPages.values.forEach({
                    assert(wrappedFile.getPage(it).traPage)
                    wrappedFile.freePage(wrappedFile.getPage(it))
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


class MVCCFile(val fileP: IMMapPageFile) : IMMapPageFile {
    override val lock: ReentrantLock
        get() = wrappedFile.lock

    override fun getPage(page: Int): MMapPageFilePage = MMapPageFilePage(this,page * PAGESIZE)

    override val fileId: FileId
        get() = wrappedFile.fileId

    override val wrappedFile: IMMapPageFile
       get() = fileP


    fun copyPageIfNecessary(idx: Long) {
        val pageNo = (idx / PAGESIZE).toInt()
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            lock.lock()
            try {
                if (!transactionInfo.getFileChangeInfo(this).pageAlreadyChanged(pageNo)) {
                    val newPage = wrappedFile.newPage()
                    wrappedFile.copy(convertOffset(pageNo * PAGESIZE), newPage.offset, PAGESIZE.toInt())
                    transactionInfo.getFileChangeInfo(this).mapPage(pageNo, newPage.number)
                    newPage.traPage = true
                    wrappedFile.setLong(newPage.offset + CHANGED_BY_TRA_INDEX, transactionInfo.baseId)
                }
            } finally {
                lock.unlock()
            }
        }
    }

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
                val currentTraId = wrappedFile.getLong(pageNo * PAGESIZE + CHANGED_BY_TRA_INDEX)
                if (currentTraId > transactionInfo.baseId) {
                    val pno = MVCC.getMinimalPreImage(this.wrappedFile, pageNo, transactionInfo.baseId)
                    if (pno != null)
                        return pno * PAGESIZE + idx % PAGESIZE
                    else
                        throw AssertionError("no preimage found, but should be there, page is newer than allowed")
                } else {
                    return idx
                }
            }
        }
        return idx
    }

    private fun convertPage(no: Int) : Int {
        return (convertOffset(no * PAGESIZE) / PAGESIZE).toInt()
    }

    override fun getByte(idx: Long) = wrappedFile.getByte(convertOffset(idx))

    override fun setByte(idx: Long, i: Byte) {
        copyPageIfNecessary(idx)
        wrappedFile.setByte(convertOffset(idx), i)
    }

    override fun getChar(idx: Long) = wrappedFile.getChar(convertOffset(idx))
    override fun setChar(idx: Long, c: Char) {
        copyPageIfNecessary(idx)
        wrappedFile.setChar(convertOffset(idx), c)
    }

    override fun getShort(idx: Long) = wrappedFile.getShort(convertOffset(idx))
    override fun setShort(idx: Long, i: Short) {
        copyPageIfNecessary(idx)
        wrappedFile.setShort(convertOffset(idx), i)
    }

    override fun getInt(idx: Long) = wrappedFile.getInt(convertOffset(idx))
    override fun setInt(idx: Long, i: Int) {
        copyPageIfNecessary(idx)
        wrappedFile.setInt(convertOffset(idx), i)
    }

    override fun getLong(idx: Long) = wrappedFile.getLong(convertOffset(idx))
    override fun setLong(idx: Long, i: Long) {
        copyPageIfNecessary(idx)
        wrappedFile.setLong(convertOffset(idx), i)
    }

    override fun getFloat(idx: Long) = wrappedFile.getFloat(convertOffset(idx))
    override fun setFloat(idx: Long, f: Float) {
        copyPageIfNecessary(idx)
        wrappedFile.setFloat(convertOffset(idx), f)
    }

    override fun getDouble(idx: Long) = wrappedFile.getDouble(convertOffset(idx))
    override fun setDouble(idx: Long, f: Double) {
        copyPageIfNecessary(idx)
        wrappedFile.setDouble(convertOffset(idx), f)
    }

    override fun getBoolean(idx: Long): Boolean = getByte(idx) != 0.toByte()
    override fun setBoolean(idx: Long, b: Boolean) = setByte(idx, (if (b) 1 else 0).toByte())

    override fun getByteArray(idx: Long, ba: ByteArray) = wrappedFile.getByteArray(idx, ba)
    override fun setByteArray(idx: Long, ba: ByteArray) {
        copyPageIfNecessary(idx)
        wrappedFile.setByteArray(convertOffset(idx), ba)
    }

    override fun copy(from: Long, to: Long, size: Int) {
        assert( size < PAGESIZE)  // prevent accidentally using this for preImaging or transaction handling
        wrappedFile.copy(convertOffset(from), convertOffset(to), size)
    }

    override fun newPage(): MMapPageFilePage {
        var result = getPage(wrappedFile.newPage().number)
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            wrappedFile.setLong(result.offset + CHANGED_BY_TRA_INDEX, transactionInfo.baseId)
            transactionInfo.getFileChangeInfo(this).mapPage(result.number, result.number)
        }
        return result
    }

    override fun freePage(page: MMapPageFilePage) {
        freePage(page.number)
    }

    /**
     * page can be relativ to the current Transaction:
     * - a preImage -> changes done on preImages not solved yet
     * - a temporary transactional image: is transient and can be freed. After the transaction the corresponding
     *      page must be freed
     * - a not anyhow mapped image
     */
    override fun freePage(page: Int) {
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            val mappedPage = convertPage(page)
            if ( wrappedFile.getLong(mappedPage * PAGESIZE + CHANGED_BY_TRA_INDEX) == transactionInfo.baseId) {
                transactionInfo.getFileChangeInfo(this).mapPage(page, null)
                // page was allocated for and during this transaction
                wrappedFile.freePage(mappedPage)
            } else {
                transactionInfo.getFileChangeInfo(this).unmapPage(page)
            }
        } else {
            wrappedFile.freePage(page)
        }
    }


    override fun isUsed(pageNum: Int): Boolean {
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            if (transactionInfo.getFileChangeInfo(this).freedPages.contains(pageNum))
                return false
        }
        return wrappedFile.isUsed(pageNum)
    }

    override fun isFree(pageNum: Int): Boolean {
        return !isUsed(pageNum)
    }

}