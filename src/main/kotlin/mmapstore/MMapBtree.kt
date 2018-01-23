package mmapstore

import java.util.*


class MMapBTreeEntry(val key: ComparableMMapPageEntry, val values: ListPageEntry, val indexEntry: MMapPageFilePage.IndexEntry?) : MMapPageEntry {
    constructor(key: ComparableMMapPageEntry, value: MMapPageEntry) : this(key, ListPageEntry(mutableListOf(value)), null)

    var childPageNumber: Int? = null  // if not null points to a page containing bigger keys then this


    // constructor(key: NioPageEntry) : this(key, null)
    override val length: Short
        get() = toShort(key.length + values.length + (if (childPageNumber != null) 4 else 0))
    override val type: NioPageEntryType
        get() = NioPageEntryType.ELSE

    override fun marshalTo(file: IMMapPageFile, offset: Long) {
        key.marshalTo(file, offset)
        values.marshalTo(file, offset + key.length)
        if (childPageNumber != null) {
            file.setInt(offset + key.length + values.length, childPageNumber ?: 0)
        }

    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MMapBTreeEntry) return false

        if (key != other.key) return false
        if (values != other.values) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }

    fun addValue(value: MMapPageEntry) {
        values.a.add(value)
    }

    fun removeValue(value: MMapPageEntry) {
        values.a.remove(value)
    }

    override fun toString(): String {
        return "NioBTreeEntry(key=$key, indexEntry=$indexEntry, childPageNumber=$childPageNumber)"
    }

}

fun unmarshallEntry(page: MMapPageFilePage, indexEntry: MMapPageFilePage.IndexEntry): MMapBTreeEntry {
    assert(!indexEntry.deleted)
    val offset = indexEntry.offsetInFile(page)
    val key = unmarshalFrom(page.file, offset)
    val values = unmarshalFrom(page.file, offset + key.length)
    val keyValueLen = key.length + values.length
    values as ListPageEntry
    val result = MMapBTreeEntry(key, values, indexEntry)
    if (keyValueLen < indexEntry.len) {
        assert(indexEntry.len - keyValueLen == 4)
        val childpage = page.file.getInt(offset + keyValueLen)
        result.childPageNumber = childpage
    }
    return result
}

fun unmarshallEntry(page: MMapPageFilePage, offset: Long): MMapBTreeEntry {
    val indexEntry = MMapPageFilePage.IndexEntry(page, offset)
    return unmarshallEntry(page, indexEntry)
}

interface IMMapBTree {
    var doCheck: Boolean
    fun insert( key: ComparableMMapPageEntry, value: MMapPageEntry)
    fun remove(key: ComparableMMapPageEntry, value: MMapPageEntry)
    fun iterator(): Iterator<MMapPageEntry>
    fun find(key: ComparableMMapPageEntry) : List<MMapPageEntry>?
    fun findSingle(key: ComparableMMapPageEntry) : MMapPageEntry?
    fun check(): String
}

class MMapBTree(val file: IMMapPageFile, rootPage: Int) : IMMapBTree {
    var root = file.getPage(rootPage)
    override var doCheck: Boolean = false
        get() = field
        set(doCheck) {
            field = doCheck;
        }

    init {
        if (!root.entries().hasNext()) {
            val leaf = file.newPage()
            val firstRootElement = MMapBTreeEntry(EmptyPageEntry(), EmptyPageEntry())
            firstRootElement.childPageNumber = leaf.number
            root.add(firstRootElement)
        } else {
            val rootEntries = getSortedEntries(root)
            assert (rootEntries.size > 0)
            assert (rootEntries[0].childPageNumber != null && rootEntries[0].key == EmptyPageEntry())
            val result = root.checkDataPage()
            assert (result.length == 0)
        }
    }

    private fun insert(page: MMapPageFilePage, toInsert: MMapBTreeEntry, forceUnique: Boolean): MMapBTreeEntry? {
        val len = toInsert.length
        val pageEntries = getSortedEntries(page)

        val greater = pageEntries.find { it.key >= toInsert.key }
        val toInsertIndex = if (greater == null) pageEntries.size else pageEntries.indexOf(greater)
        if (greater != null && greater.key == toInsert.key) {
            assert(!forceUnique, { "trying to reinsert but key is yet there" })
            // the key is found in the current page, multiples are allowed, so add the value to the BTreeEntryValues-List
            if (page.allocationFitsIntoPage(toInsert.values.a[0].length)) {
                greater.addValue(toInsert.values.a[0])
                if (greater.indexEntry == null)
                    throw AssertionError("should not be null here")
                else
                    page.remove(greater.indexEntry)
                page.add(greater)
            } else {
                throw NotImplementedError("TODO: handle split or link to extra pages because of additional value not fitting into page")
                // TODO: handle split or link to extra pages because of additional value not fitting into page
            }
        } else
            if (greater == null && pageEntries.size == 0
                    || greater == null && pageEntries.first().childPageNumber == null
                    || greater != null && greater.childPageNumber == null) {
                return insertAndSplitIfNecessary(page, toInsert, toInsertIndex, pageEntries, false)
            } else {
                // found in inner Node
                assert(
                        greater == null && pageEntries.first().childPageNumber != null
                                || greater != null && greater.childPageNumber != null)
                assert(toInsertIndex > 0)  // leftmost element must be smaller than all
                if (toInsert.childPageNumber == null) {
                    // need to go until found position in leaf
                    val childPageNumber = pageEntries[toInsertIndex - 1].childPageNumber
                    if (childPageNumber == null)
                        throw AssertionError("expected childpagenumber to be != null in inner node")
                    else {
                        val nextLayerPage = file.getPage(childPageNumber)
                        val result = insert(nextLayerPage, toInsert, forceUnique)
                        if (result != null) {
                            if (result.key == EmptyPageEntry()) {
                                // need to replace childPageNumber
                                pageEntries[toInsertIndex - 1].childPageNumber = result.childPageNumber
                                page.remove(pageEntries[toInsertIndex - 1].indexEntry!!)
                                page.add(pageEntries[toInsertIndex - 1])
                                return null
                            } else {
                                // insert this in current page since a split has occurred
                                return insert(page, result, forceUnique)
                            }
                        }
                    }
                } else {
                    // insert into inner page
                    return insertAndSplitIfNecessary(page, toInsert, toInsertIndex, pageEntries, true)
                }
            }
        return null
    }

    private fun getSortedEntries(page: MMapPageFilePage): MutableList<MMapBTreeEntry> {
        val pageEntries = mutableListOf<MMapBTreeEntry>()
        page.indexEntries().forEach {
            if (!it.deleted)
                pageEntries.add(unmarshallEntry(page, it))
        }

        pageEntries.sortBy({ it.key })
        return pageEntries
    }

    private fun ifEmptyInnerPageReturnFirstElement(page: MMapPageFilePage): MMapBTreeEntry? {
        if (page.indexEntries().asSequence().count({ !it.deleted }) == 1) {
            val result = unmarshallEntry(page,
                    page.indexEntries()
                            .asSequence()
                            .filter { !it.deleted }
                            .take(1)
                            .first())
            if (result.childPageNumber != null)
                return result
        }
        return null;
    }

    private fun insertAndSplitIfNecessary(page: MMapPageFilePage,
                                          toInsert: MMapBTreeEntry,
                                          toInsertIndex: Int,
                                          pageEntries: MutableList<MMapBTreeEntry>,
                                          isInnerPage: Boolean): MMapBTreeEntry? {
        if (page.allocationFitsIntoPage((toInsert.length + PAGESIZE / 3).toInt())
        /* || page.allocationFitsIntoPage(toInsert.length) && pageEntries.size <= 10*/) {
            page.add(toInsert)
            return null
        } else {
            pageEntries.add(toInsertIndex, toInsert)
            val completeLength = pageEntries.sumBy { it.length.toInt() } + toInsert.length
            var currentSum = 0
            var splitEntry: MMapBTreeEntry? = null

            var insertLeft = false

            val newPage = file.newPage()
            for (e in pageEntries.iterator()) {
                currentSum += e.length
                if (currentSum >= completeLength / 2) {
                    // limit for left page reached
                    // first note split Entry that should be propagated
                    if (splitEntry == null) {
                        // this entry will be returned to be inserted into the parent page
                        splitEntry = e
                        if (isInnerPage) {
                            assert(e.childPageNumber != null)
                            val firstEntry = MMapBTreeEntry(EmptyPageEntry(), EmptyPageEntry())
                            // save here childPageNumber of splitEntry, so before first element in page,
                            // all elements of page on which the splitEntry points are positions
                            firstEntry.childPageNumber = splitEntry.childPageNumber
                            newPage.add(firstEntry)
                        }
                        splitEntry.childPageNumber = newPage.number
                    } else {
                        // then add entry to newpage, because it is right from the splitEntry
                        newPage.add(e)
                    }

                    if (!(e === toInsert) && e.indexEntry != null) {
                        // remove everything right of limit (including splitentry), except if newly to be inserted (not yet in tree)
                        page.remove(e.indexEntry)
                    }
                } else {
                    if (e === toInsert)
                    // don't add new entry yet, since page-index might get compacted during add and btw.
                    // can't add otherwise the split wasn't necessary
                        insertLeft = true
                }
            }
            // entry to be inserted found on the left side during limit calculation
            if (insertLeft) {
                // so the new entry not inserted yet during splitting
                page.add(toInsert)
            }
            return splitEntry
        }
    }



    override fun insert(key: ComparableMMapPageEntry, value: MMapPageEntry) {
        val toInsert = MMapBTreeEntry(key, value)

        insertAndFixRoot(toInsert, false)
        if (doCheck) {
            val message = check()
            if (message.length > 0) println(message)
        }
    }

    private fun insertAndFixRoot(toInsert: MMapBTreeEntry, forceUnique: Boolean) {
        val splitElement = insert(root, toInsert, forceUnique)
        fixRoot(splitElement)
    }

    // make sure that root when set once stays the same page.
    private fun fixRoot(splitElement: MMapBTreeEntry?) {
        if (splitElement != null) {
            assert (splitElement.key != EmptyPageEntry())
            val tmp = root
            if (tmp != null) {
                val newLeftChild = file.newPage()
                val leftChildEntries = getSortedEntries(tmp)
                for (e in leftChildEntries) {
                    newLeftChild.add(e)
                }
                tmp.clearContents()
                val firstRootElement = MMapBTreeEntry(EmptyPageEntry(), EmptyPageEntry())
                firstRootElement.childPageNumber = newLeftChild.number
                tmp.add(firstRootElement)
                tmp.add(splitElement)
            } else {
                throw AssertionError("expect root in Btree to be initialized")
            }

        } else {
            val tmp = root
            if (tmp != null) {
                val inner = ifEmptyInnerPageReturnFirstElement(tmp)
                if (inner != null) {
                    val childPageNumber = inner.childPageNumber
                    if (childPageNumber != null) {
                        val soloChild = file.getPage(childPageNumber)
                        val soloChildEntries = getSortedEntries(soloChild)
                        assert(soloChildEntries.size > 0)
                        if (soloChildEntries[0].childPageNumber != null) {
                            tmp.clearContents()
                            for (e in soloChildEntries) {
                                tmp.add(e)
                            }
                            file.freePage(soloChild)
                        }
                    }
                }
            } else {
                throw AssertionError("expect root in Btree to be initialized")
            }
        }
    }

    private fun delete(page: MMapPageFilePage, toDelete: ComparableMMapPageEntry, value: MMapPageEntry, toReInsert: MutableList<MMapBTreeEntry>, level: Int): Pair<Boolean, MMapBTreeEntry?> {
        val pageEntries = getSortedEntries(page)
        val greater = pageEntries.find { it.key >= toDelete }
        val index = if (greater == null) pageEntries.size else pageEntries.indexOf(greater)
        if (greater != null && greater.key != toDelete || greater == null) {  // not found yet
            assert(index > 0)
            val referingEntry = pageEntries[index - 1]
            val nextChildPageNo = referingEntry.childPageNumber ?: throw IndexOutOfBoundsException("entry to be deleted not found in tree")
            val child = file.getPage(nextChildPageNo)
            val result = delete(child, toDelete, value, toReInsert, level + 1)
            val newEntry = result.second
            if (newEntry != null) {
                assert(result.first)
                return Pair(true, insert(page, newEntry, false))
            } else {
                if (result.first)  // make sure to don't handle anything further since a split occured during the call with possible changes
                    return Pair(true, null)
            }
            if (child.freeSpace() > (PAGESIZE.toInt()) * 2 / 3) {
                if (child.empty()) {
                    handleEmptyChildPage(page, child, pageEntries, index - 1, toReInsert)
                    return Pair(false, null)
                }
                // try to remove child or the right neighbour by merging
                if (index > 1) {
                    if (!tryMergeChildToLeftPage(pageEntries, index, child, page)) {
                        if (index < pageEntries.size) {
                            tryMergeRightToChild(pageEntries, index, child, page)
                        }
                    }
                } else if (index < pageEntries.size) {
                    tryMergeRightToChild(pageEntries, index, child, page)
                }
            }
        } else {
            val greaterIndexEntry = greater.indexEntry ?: throw AssertionError("sorted pageentries generated with indexentry null")
            val orgSize = greater.values.a.size
            if (value !is EmptyPageEntry)
                greater.values.a.remove(value)

            if (greater.values.a.size < orgSize || value is EmptyPageEntry) {
                if (greater.values.a.size == 0 || value is EmptyPageEntry) {
                    // entry has been found and can be deleted
                    val nextChildPageNo = pageEntries[index].childPageNumber
                    if (nextChildPageNo == null) {
                        page.remove(greater.indexEntry)
                    } else {
                        val child = file.getPage(nextChildPageNo)

                        var entryForReplacement = findSmallestEntry(child)
                        if (entryForReplacement == null) {
                            file.freePage(child)
                            page.remove(greater.indexEntry)
                        } else {
                            // now removeButKeepIndexEntry it from child, be aware, that delete could have lead to split
                            val replDeleteResult = delete(child, entryForReplacement.key, EmptyPageEntry(), toReInsert, level+1)
                            entryForReplacement.childPageNumber = greater.childPageNumber
                            page.remove(greater.indexEntry)
                            if (replDeleteResult.first) {
                                val newEntry = replDeleteResult.second
                                // deleting entryForReplacement has led to a split, but it stays the smallest element
                                if (!page.allocationFitsIntoPage(entryForReplacement.length)) {
                                    if(newEntry != null) {
                                        assert(newEntry.key > entryForReplacement.key)
                                        if (page.allocationFitsIntoPage(newEntry.length)) {
                                            page.add(newEntry)
                                        } else {
                                            assert(false, { "expected this never to occur"})
                                        }
                                    }
                                    println("doing split")
                                    pageEntries.removeAt(index)
                                    return Pair(true, insertAndSplitIfNecessary(page, entryForReplacement, index, pageEntries, true))
                                } else {
                                    page.add(entryForReplacement)
                                    if (newEntry != null) {
                                        assert(newEntry.key > entryForReplacement.key)
                                        return Pair(true, insert(page, newEntry, false))
                                    } else {
                                        return Pair(true, null)
                                    }
                                }
                            }

                            if (child.freeSpace() > (PAGESIZE.toInt()) * 2 / 3) {
                                if (child.empty()) {
                                    file.freePage(child)
                                    entryForReplacement.childPageNumber = null
                                    toReInsert.add(entryForReplacement)
                                    return Pair(false, null)
                                }
                                assert(index > 0)
                                val prevChildPageNo = pageEntries[index - 1].childPageNumber
                                val leftPage = (if (prevChildPageNo == null) null else file.getPage(prevChildPageNo)) ?: throw AssertionError("expected left page to be available for merging")

                                // TODO: do exact calculation of: page fits in predecessor (use freeindexentries)
                                // leftPage.compactIndexArea()
                                // child.compactIndexArea()

                                if (leftPage.freeSpace() - entryForReplacement.length - INDEX_ENTRY_SIZE > (
                                        PAGESIZE - child.freeSpace())) {
                                    // should fit
                                    entryForReplacement.childPageNumber = null  // must be set by inner page, if it is one
                                    // println("1merging ${child.number} into $prevChildPageNo parent: ${page.number}")
                                    for (e in child.indexEntries()) {
                                        if (!e.deleted) {
                                            val entry = unmarshallEntry(child, e)
                                            if (entry.childPageNumber != null && entry.key == EmptyPageEntry()) {
                                                entryForReplacement.childPageNumber = entry.childPageNumber
                                            } else if (leftPage.allocationFitsIntoPage(entry.length)) {
                                                leftPage.add(entry)
                                                child.remove(e)
                                            } else {
                                                throw AssertionError("all should fit into left")
                                            }
                                        }
                                    }
                                    leftPage.add(entryForReplacement)
                                    file.freePage(child)
                                } else {
                                    if (!page.allocationFitsIntoPage(entryForReplacement.length)) {
                                        println("doing split")
                                        pageEntries.removeAt(index)
                                        return Pair(true, insertAndSplitIfNecessary(page, entryForReplacement, index, pageEntries, true))
                                    } else {
                                        page.add(entryForReplacement)
                                    }
                                }

                            } else {
                                if (!page.allocationFitsIntoPage(entryForReplacement.length)) {
                                    println("doing split")
                                    pageEntries.removeAt(index)
                                    return Pair(true, insertAndSplitIfNecessary(page, entryForReplacement, index, pageEntries, true))
                                } else {
                                    page.add(entryForReplacement)
                                }
                            }
                        }
                    }
                } else {  // remove this value, entry, linkage, ... stay except if merge is necessary
                    page.remove(greater.indexEntry)
                    page.add(greater)
                }
            } else {
                throw IndexOutOfBoundsException("value not found associated with key")
            }
        }
        return Pair(false, null)
    }

    private fun handleEmptyChildPage(page: MMapPageFilePage, child: MMapPageFilePage, pageEntries: MutableList<MMapBTreeEntry>, index: Int, toReInsert: MutableList<MMapBTreeEntry>) {
        val referingEntry = pageEntries[index]
        if (index == 0) {
            if (pageEntries.size == 1) {
                page.remove(referingEntry.indexEntry!!)
                // this page is now empty
            } else {
                // in this case delete second entry and reinsert it.
                // the page the second entry pointed to must be refered to by the first
                referingEntry.childPageNumber = pageEntries[1].childPageNumber
                pageEntries[1].childPageNumber = null
                page.remove(pageEntries[1].indexEntry!!)
                page.remove(referingEntry.indexEntry!!)
                page.add(referingEntry)
                toReInsert.add(pageEntries[1])
            }
        } else {

            page.remove(referingEntry.indexEntry!!)
            referingEntry.childPageNumber = null
            toReInsert.add(referingEntry)
        }
        file.freePage(child)
    }

    private fun tryMergeChildToLeftPage(pageEntries: MutableList<MMapBTreeEntry>, index: Int, child: MMapPageFilePage, page: MMapPageFilePage): Boolean {
        val rightEntry = pageEntries[index - 1]
        // child is small, try to add it to the left
        val prevChildPageNo = pageEntries[index - 2].childPageNumber ?: throw AssertionError("expected left page to be available for merging")
        val leftPage = file.getPage(prevChildPageNo)

        // TODO: do exact calculation of: page fits in predecessor (use freeindexentries)
        return mergeRightToLeft(leftPage, child, rightEntry, page)
    }

    private fun tryMergeRightToChild(pageEntries: MutableList<MMapBTreeEntry>, index: Int, child: MMapPageFilePage, page: MMapPageFilePage): Boolean {
        val rightEntry = pageEntries[index]
        // leftmost page is small, try to add everything from the right page
        val rightPage = file.getPage(rightEntry.childPageNumber!!)
        // TODO: do exact calculation of: page fits in predecessor (use freeindexentries)
        return mergeRightToLeft(child, rightPage, rightEntry, page)
    }

    private fun mergeRightToLeft(leftPage: MMapPageFilePage, rightPage: MMapPageFilePage, rightEntry: MMapBTreeEntry, page: MMapPageFilePage): Boolean {
        leftPage.compactIndexArea()
        rightPage.compactIndexArea()

        if (leftPage.freeSpace() - rightEntry.length - INDEX_ENTRY_SIZE >
                (PAGESIZE - rightPage.freeSpace())) {
            rightEntry.childPageNumber = null
            // println("2merging ${rightPage.number} into ${leftPage.number} parent: ${page.number}")
            for (e in rightPage.indexEntries()) {
                if (!e.deleted) {
                    val entry = unmarshallEntry(rightPage, e)
                    if (entry.childPageNumber != null && entry.key == EmptyPageEntry()) {
                        rightEntry.childPageNumber = entry.childPageNumber
                    } else if (leftPage.allocationFitsIntoPage(entry.length)) {
                        leftPage.add(entry)
                        rightPage.remove(e)
                    } else {
                        throw AssertionError("all should fit into left")
                    }
                }
            }
            leftPage.add(rightEntry)

            page.remove(rightEntry.indexEntry!!)
            file.freePage(rightPage)
            return true
        } else { // move entries from right to left if possible
            println("trying to distribute between ${leftPage.number} and ${rightPage.number} for ${page.number}")
            assert(rightEntry.childPageNumber != null && rightEntry.childPageNumber == rightPage.number)
            var rightPageEntries = getSortedEntries(rightPage)
            var leftPageEntries = getSortedEntries(leftPage)
            val firstChildRight = rightPageEntries.first().childPageNumber
            val lenright = rightPageEntries.sumBy { it.length.toInt() }
            val lenleft = leftPageEntries.sumBy { it.length.toInt() }
            if (lenleft > lenright) {
                println("decided to move from left to right")
                // move from left to right
                leftPageEntries.add(rightEntry)
                var tmp = lenright
                // first candidates to make right have less just more than 2/3 of PAGESIZE used, since one of it is moved to parent...ok
                val toMove = leftPageEntries.reversed().takeWhile { tmp += it.length.toInt(); tmp < PAGESIZE * 2 / 3 }.reversed()
                // the search in candidates until entry fits into parent page
                val toRemove = toMove.takeWhile { page.freeSpace() + rightEntry.length - it.length < 0 }
                // remove everything that does not fit
                var newToMove = toMove.drop(toRemove.size)
                if (newToMove.size > 1) {
                    // non trivial
                    val splitEntry = newToMove.first()
                    if (firstChildRight != null) {
                        val first = rightPageEntries.first()
                        rightPage.remove(first.indexEntry!!)
                        first.childPageNumber = splitEntry.childPageNumber
                        rightPage.add(first)
                    }
                    leftPage.remove(splitEntry.indexEntry!!)
                    newToMove.drop(1).forEach({
                        if (it != rightEntry)
                            leftPage.remove(it.indexEntry!!)
                        else {
                            rightEntry.childPageNumber = firstChildRight
                            page.remove(rightEntry.indexEntry!!)
                        }
                        rightPage.add(it)
                    })
                    splitEntry.childPageNumber = rightPage.number
                    page.add(splitEntry)
                    println("did move from left to right")
                    check()
                    return true
                }
                println("did not move from left to right")
            } else {  // move from right to left
                println("decided to move from right to left")
                val rightEmpty = if (firstChildRight != null) rightPageEntries.removeAt(0) else null
                rightPageEntries.add(0, rightEntry)
                var tmp = lenleft
                // first candidates to make right have less just more than 2/3 of PAGESIZE used, since one of it is moved to parent...ok
                val toMove = rightPageEntries.takeWhile { tmp += it.length.toInt(); tmp < PAGESIZE * 2 / 3 }
                // the search in candidates until entry fits into parent page
                val toRemove = toMove.reversed().takeWhile { page.freeSpace() + rightEntry.length - it.length < 0 }
                var newToMove = toMove.drop(toRemove.size)
                if (newToMove.size > 1) {
                    // non trivial
                    val splitEntry = newToMove.last()
                    rightPage.remove(splitEntry.indexEntry!!)
                    newToMove.dropLast(1).forEach({
                        if (it != rightEntry)
                            rightPage.remove(it.indexEntry!!)
                        else {
                            rightEntry.childPageNumber = firstChildRight
                            page.remove(rightEntry.indexEntry!!)
                        }
                        leftPage.add(it)
                    })
                    if (firstChildRight != null) {
                        val first = MMapBTreeEntry(EmptyPageEntry(), EmptyPageEntry())
                        first.childPageNumber = splitEntry.childPageNumber
                        rightPage.remove(rightEmpty!!.indexEntry!!)
                        rightPage.add(first)
                    }
                    splitEntry.childPageNumber = rightPage.number
                    page.add(splitEntry)
                    println("did move from right to left")
                    check()
                    return true
                }
                println("did not move from right to left")
            }
            return false
        }
    }


    private fun findSmallestEntry(child: MMapPageFilePage): MMapBTreeEntry? {
        val entries = getSortedEntries(child)
        if (entries.size == 0)
            return null
        val childPageNumber = entries[0].childPageNumber
        if (childPageNumber == null)
            return entries[0]
        else {
            val res = findSmallestEntry(file.getPage(childPageNumber))
            if (res == null && entries.size > 1)
                return entries[1]
            else return res
        }
    }

    private fun getNextValidIndexEntry(childIndexEntries: Iterator<MMapPageFilePage.IndexEntry>): MMapPageFilePage.IndexEntry {
        while (childIndexEntries.hasNext()) {
            val e = childIndexEntries.next()
            if (!e.deleted)
                return e
        }

        throw AssertionError("expecting at least one valid entry")
    }

    override fun remove(key: ComparableMMapPageEntry, value: MMapPageEntry) {
        val toReInsert = mutableListOf<MMapBTreeEntry>()
        val splitElement = delete(root, key, value, toReInsert, 0).second
        fixRoot(splitElement)
        for (e in toReInsert) {
            insertAndFixRoot(e, true)
        }

        if (doCheck) {
            val message = check()
            if (message.length > 0) println(message)
        }

    }

    override fun iterator(): Iterator<MMapPageEntry> {

        return object : Iterator<MMapPageEntry> {
            val path = Stack<Pair<MMapPageFilePage, Int>>()

            init {
                val p = Pair(file.getPage(root!!.number), 0)
                path.push(p)
            }

            override fun hasNext(): Boolean {
                while (true) {
                    val top = path.pop()
                    val entries = getSortedEntries(top.first)
                    if (top.second < entries.size) {
                        val act = entries[top.second]

                        val childPageNumber = act.childPageNumber
                        if (childPageNumber != null && top.second == 0) {
                            val child = file.getPage(childPageNumber)
                            path.push(top)  // restore parent
                            path.push(Pair(child, 0))
                        } else {
                            path.push(top)
                            return true
                        }
                    } else {
                        if (path.size == 0)
                        // reached the top where nothing is left
                            return false
                        else {
                            val parent = path.pop()
                            path.push(Pair(parent.first, parent.second + 1))
                        }
                    }
                }
            }

            override fun next(): MMapPageEntry {
                if (hasNext()) {
                    val top = path.pop()
                    val entries = getSortedEntries(top.first)
                    val result = entries[top.second]
                    val childPageNumber = result.childPageNumber
                    if (childPageNumber != null) {
                        val child = file.getPage(childPageNumber)
                        path.push(top)
                        path.push(Pair(child, 0))
                    } else {
                        path.push(Pair(top.first, top.second + 1))
                    }
                    return result
                } else {
                    throw IndexOutOfBoundsException("BTree Iterator, next called but nothing left")
                }
            }

        }
    }

    fun find(page: MMapPageFilePage, key: ComparableMMapPageEntry) : MMapBTreeEntry? {
        val entries = getSortedEntries(page)
        if (entries.size == 0)
            return null
        val res = entries.binarySearch { it.key.compareTo(key) }
        if (res >= 0) {
            return entries[res]
        } else {
            if (entries[0].childPageNumber == null) {
                return null
            } else {
                assert(res < -1)
                val childPageNumber = entries[-res - 2].childPageNumber
                if (childPageNumber != null)
                    return find(file.getPage(childPageNumber), key)
                else
                    return null
            }
        }
    }

    override fun find(key: ComparableMMapPageEntry) : List<MMapPageEntry>? {
        var res = find(root, key)
        if (res != null) {
            if (res.values is ListPageEntry) {
                return res.values.a
            } else {
                return listOf(res.values)
            }
        } else
            return null
    }

    override fun findSingle(key: ComparableMMapPageEntry) : MMapPageEntry? {
        var tmp = find(key)
        if (tmp != null) {
            assert (tmp.size <= 1)
            if (tmp.size == 1) {
                return tmp[0]
            } else {
                return null
            }
        } else
            return null
    }

    override fun check(): String {
        fun checkPage(page: MMapPageFilePage, smallerEntryKey: ComparableMMapPageEntry, biggerEntryKey: ComparableMMapPageEntry?, done: MutableSet<Int>, result: StringBuffer) {
            result.append(page.checkDataPage())
            // println("Doing pg.Page: ${page.number}")
            val entries = getSortedEntries(page)
            if (entries.size == 0) {
                // result.append("page(${page.number}):Empty Leaf pg.Page\n")
            } else {
                val firstChildPageNumber = entries[0].childPageNumber
                if (firstChildPageNumber != null) {
                    if (entries[0].key != EmptyPageEntry()) {
                        result.append("page(${page.number}):Expected empty first key in page ${page.number}\n")
                    }
                    if (entries[0].values.length != 4.toShort()) {
                        result.append("page(${page.number}):Expected length of 4 in values in first entry of page ${page.number}\n")
                    }
                    if (file.getPage(firstChildPageNumber!!).empty()) {
                       //  result.append("page(${page.number}):First innerpage entry has empty leaf child\n")
                    }

                    var previousEntryKey = smallerEntryKey
                    for (i in 1..entries.size - 1) {
                        val nioBTreeEntry = entries[i]

                        if (nioBTreeEntry.key <= smallerEntryKey) {
                            result.append("page(${page.number}):Expected $nioBTreeEntry to be bigger than $smallerEntryKey\n")
                        }
                        if (nioBTreeEntry.key <= previousEntryKey) {
                            result.append("page(${page.number}):Expected $nioBTreeEntry to be bigger than $previousEntryKey\n")
                        }
                        if (biggerEntryKey != null) {
                            if (nioBTreeEntry.key >= biggerEntryKey)
                                result.append("page(${page.number}):Expected $nioBTreeEntry to be smaller than $biggerEntryKey\n")
                        }
                        if (nioBTreeEntry.key == EmptyPageEntry()) {
                            result.append("page(${page.number}):Invalid entry $nioBTreeEntry may not be EmptyEntry\n")
                        }
                        val childPageNumber = nioBTreeEntry.childPageNumber
                        if (childPageNumber == null) {
                            result.append("page(${page.number}): Invalid entry $nioBTreeEntry no childpage reference\n")
                        }
                        previousEntryKey = nioBTreeEntry.key
                    }
                    // result.append("checking childs of ${page.number}\n")
                    for (i in 0..entries.size - 1) {
                        val childPageNumber = entries[i].childPageNumber
                        if (childPageNumber != null) {
                            if (file.isUsed(childPageNumber)) {
                                val nextSmaller = if (i == 0) smallerEntryKey else entries[i].key
                                val nextBigger = if (i + 1 < entries.size) entries[i + 1].key else biggerEntryKey
                                checkPage(file.getPage(childPageNumber), nextSmaller, nextBigger, done, result)
                            } else
                                result.append("page(${page.number}): refered childpage: $childPageNumber is marked as free\n")
                        }
                    }
                } else {
                    var previousEntryKey = smallerEntryKey
                    for (i in 0..entries.size - 1) {
                        val nioBTreeEntry = entries[i]
                        val childPageNumber = nioBTreeEntry.childPageNumber
                        if (childPageNumber != null) {
                            result.append("page(${page.number}): Invalid leaf entry $nioBTreeEntry has childpage reference to $childPageNumber\n")
                        }
                        if (previousEntryKey >= nioBTreeEntry.key) {
                            result.append("page(${page.number}): Expected $nioBTreeEntry to be bigger than $previousEntryKey\n")
                        }
                        if (biggerEntryKey != null && biggerEntryKey <= nioBTreeEntry.key) {
                            result.append("page(${page.number}): Expected $nioBTreeEntry to be smaller than $biggerEntryKey\n")
                        }
                        previousEntryKey = nioBTreeEntry.key

                    }
                }
                if (done.contains(page.number)) {
                    result.append("page(${page.number}): handled more than once\n")
                } else {
                    done.add(page.number)
                }
            }
            // result.append("Ready  pg.Page: ${page.number}\n")
        }

        val result = StringBuffer()
        checkPage(root!!, EmptyPageEntry(), null, mutableSetOf(), result)
        return result.toString()
    }

}