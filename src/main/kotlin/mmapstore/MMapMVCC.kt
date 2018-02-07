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

        // originalNo == copyNo if it has no original, i.e. split during insert
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

    var committingId = baseId
    var committing: Boolean
        get() { return committingId > baseId }
        set(value) {
            if (value)
                committingId = MVCC.nextTra(id)
            else
                committingId = baseId
        }

    var rollingback = false


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
    val lock = ReentrantLock()
    var startTime = java.lang.System.currentTimeMillis()
    val changes = AtomicLong(0)
    val transactionIds = AtomicInteger(0)
    val preImagesPerFile = mutableMapOf<IMMapPageFile,MutableMap<Int, MutableList<TraIdPageNo>>>()

    var threadLocalTransaction: ThreadLocal<TransactionInfo> = ThreadLocal()
    val transactions = mutableMapOf<Short,TransactionInfo>()
    val threadsForTransactions = mutableMapOf<Short, Long>()

    var lastTraId = 0L

    fun init() {
        threadLocalTransaction = ThreadLocal()
        changes.set(0)
        transactionIds.set(0)
        startTime = System.currentTimeMillis()
        preImagesPerFile.clear()
        transactions.clear()
        threadsForTransactions.clear()
    }

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
        assert (!file.getPage(preImageNo).preImage)
        file.getPage(preImageNo).preImage = true
        val preImagesOfFile = getPreImagesOfFile(file)
        synchronized(preImagesOfFile) {
            var preImagesOfPage = preImagesOfFile[pageNo]
            if (preImagesOfPage == null) {
                preImagesOfPage = mutableListOf()
                preImagesOfFile[pageNo] = preImagesOfPage
            }
            preImagesOfPage.add(TraIdPageNo(traId, preImageNo, lastTraId))
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

        try {
            lock.lock()
            try {
                transactionInfo.committing = true
                transactionInfo.btreeOperations.forEach {
                    when (it.op) {
                        BTreeOperationTypes.INSERT -> it.btree.insert(it.key, it.value)
                        BTreeOperationTypes.DELETE -> it.btree.remove(it.key, it.value)
                    }
                }
            } finally {
                transactionInfo.committing = false
                lock.unlock()
            }
            /*
            // save all changed pages as preImages, so that current running transactions
            // can yet read expected data.
            transactionInfo.fileChangeInfo.entries.forEach({
                val wrappedFile = it.key.wrappedFile
                wrappedFile.lock.lock()
                val traId = nextTra(transactionInfo.baseId)
                try {
                    it.value.changedPages.entries.forEach({
                        val original = wrappedFile.getPage(it.key)
                        if (it.key != it.value) {
                            val orgId = wrappedFile.getLong(original.offset + CHANGED_BY_TRA_INDEX)
                            assert (!original.traPage) // trapages are exclusive per transaction, cannot be original
                            // at the moment: can only handle not overlapping changes
                            assert(orgId < transactionInfo.baseId)
                            // save preimage of original
                            if (MVCC.getPreImage(wrappedFile, it.key, orgId) == null) {
                                val preImage = wrappedFile.newPage()
                                println("creating preimage ${preImage.number} for ${original.number}")
                                wrappedFile.copy(original.offset, preImage.offset, PAGESIZE.toInt())
                                MVCC.addPreImage(wrappedFile, it.key, orgId, preImage.number)
                            }
                            val copy = wrappedFile.getPage(it.value)
                            assert (copy.traPage)
                            copy.traPage = false  // make sure it will not be seen in original
                            wrappedFile.copy(copy.offset, original.offset, PAGESIZE.toInt())
                            wrappedFile.setLong(original.offset + CHANGED_BY_TRA_INDEX, traId)
                            wrappedFile.freePage(copy)
                        } else {
                            // page newly necessary in this transaction, so clear only traPage-Bit
                            assert(original.traPage)
                            original.traPage = false
                        }
                    })
                } finally {
                    wrappedFile.lock.unlock()
                }
            })
            */
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
                            assert(file.getPage(it.pageNo).preImage)
                            file.freePage(it.pageNo)
                            preImages.remove(it)
                        })
                    })

                } finally {
                    file.lock.unlock()
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
    override val file = btree.file
    override var doCheck: Boolean
        get() = btree.doCheck
        set(value) {btree.doCheck = value}

    override fun insert(key: ComparableMMapPageEntry, value: MMapPageEntry) {
        val currentTransaction = MVCC.getCurrentTransaction()
        if (currentTransaction != null) {
            currentTransaction.btreeOperations.add(BTreeOperation(btree, BTreeOperationTypes.INSERT, key, value))
        }
        btree.insert(key, value)
    }

    override fun remove(key: ComparableMMapPageEntry, value: MMapPageEntry) {
        val currentTransaction = MVCC.getCurrentTransaction()
        if (currentTransaction != null) {
            currentTransaction.btreeOperations.add(BTreeOperation(btree, BTreeOperationTypes.DELETE, key, value))
        }
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
                if (transactionInfo.committing) {
                    savePreImageDuringCommit(pageNo, transactionInfo)
                } else if (!transactionInfo.getFileChangeInfo(this).pageAlreadyChanged(pageNo)) {
                    assert(!wrappedFile.getPage(pageNo).traPage)
                    assert(!wrappedFile.getPage(pageNo).preImage)  // not supported yet
                    val newPage = wrappedFile.newPage()
                    wrappedFile.copy(convertOffset(pageNo * PAGESIZE), newPage.offset, PAGESIZE.toInt())
                    transactionInfo.getFileChangeInfo(this).mapPage(pageNo, newPage.number)
                    newPage.traPage = true
                    wrappedFile.setLong(newPage.offset + CHANGED_BY_TRA_INDEX, transactionInfo.baseId)
                    println("copied page if necessary: $pageNo to ${newPage.number}")
                }
            } finally {
                lock.unlock()
            }
        }
    }

    private fun savePreImageDuringCommit(pageNo: Int, transactionInfo: TransactionInfo) {
        val original = MMapPageFilePage(wrappedFile, pageNo)
        val orgId = wrappedFile.getLong(original.offset + CHANGED_BY_TRA_INDEX)
        if (orgId < transactionInfo.committingId) {
            if (MVCC.getPreImage(wrappedFile, pageNo, transactionInfo.baseId) == null) {
                val preImage = wrappedFile.newPage()
                println("creating preimage during commit ${preImage.number} for ${pageNo}")
                wrappedFile.copy(original.offset, preImage.offset, PAGESIZE.toInt())
                MVCC.addPreImage(wrappedFile, pageNo, orgId, preImage.number)
            }
            wrappedFile.setLong(original.offset + CHANGED_BY_TRA_INDEX, transactionInfo.committingId)
        } else {
            assert(orgId == transactionInfo.committingId)
        }
    }

    private fun convertOffset(idx: Long): Long {
        var pageNo = (idx / PAGESIZE).toInt()

        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            var lastTra = transactionInfo.baseId
            if (transactionInfo.committing) {
                val pageTraId = wrappedFile.getLong(pageNo * PAGESIZE + CHANGED_BY_TRA_INDEX)
                assert (pageTraId <= transactionInfo.committingId)
                return idx
            } else {
                var maxChange = Long.MAX_VALUE
                val pno = transactionInfo.getFileChangeInfo(this).changedPages[pageNo]
                if (pno != null) {
                    // if (pno.toLong() != idx / PAGESIZE)
                    // println("converted Offset1: ${pno!! * PAGESIZE + idx % PAGESIZE} page: ${pno} was $pageNo")
                    return pno * PAGESIZE + idx % PAGESIZE
                } else {
                    val currentTraId = wrappedFile.getLong(pageNo * PAGESIZE + CHANGED_BY_TRA_INDEX)
                    if (currentTraId > transactionInfo.baseId) {
                        val pno = MVCC.getMinimalPreImage(this.wrappedFile, pageNo, transactionInfo.baseId)
                        if (pno!!.toLong() != idx / PAGESIZE)
                            println("converted Offset2: ${pno!! * PAGESIZE + idx % PAGESIZE} page: ${pno} was $pageNo")
                        if (pno != null)
                            return pno * PAGESIZE + idx % PAGESIZE
                        else
                            throw AssertionError("no preimage found, but should be there, page is newer than allowed")
                    } else {
                        return idx
                    }
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

    override fun getByteArray(idx: Long, ba: ByteArray) = wrappedFile.getByteArray(convertOffset(idx), ba)
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
            if (transactionInfo.committing) {
                wrappedFile.setLong(result.offset + CHANGED_BY_TRA_INDEX, transactionInfo.committingId)
            } else {
                wrappedFile.setLong(result.offset + CHANGED_BY_TRA_INDEX, transactionInfo.baseId)
                transactionInfo.getFileChangeInfo(this).mapPage(result.number, result.number)
                result.traPage = true
            }
        }
        println("New Page: ${result.number}")
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
        println("freePage: $page")
        val transactionInfo = MVCC.getCurrentTransaction()
        if (transactionInfo != null) {
            if (transactionInfo.committing) {
                savePreImageDuringCommit(page,transactionInfo)
                wrappedFile.freePage(page)
            } else {
                val mappedPage = convertPage(page)
                println("freePage: mapped to: $mappedPage")
                if (wrappedFile.getLong(mappedPage * PAGESIZE + CHANGED_BY_TRA_INDEX) == transactionInfo.baseId) {
                    transactionInfo.getFileChangeInfo(this).unmapPage(page)
                    // page was allocated for and during this transaction
                    wrappedFile.freePage(mappedPage)
                    println("actually freed Page: $mappedPage")
                } else {
                    transactionInfo.getFileChangeInfo(this).unmapPage(page)
                }
            }
        } else {
            wrappedFile.freePage(page)
            println("actually freed Page: $page")
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