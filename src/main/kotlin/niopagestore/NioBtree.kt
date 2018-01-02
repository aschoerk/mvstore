package niopagestore

import nioobjects.TXIdentifier
import niopageentries.*
import niopageobjects.NioPageFile
import niopageobjects.NioPageFilePage
import niopageobjects.PAGESIZE


class NioBTreeEntry(val key: NioPageEntry, val values: ListPageEntry, val indexEntry: NioPageFilePage.IndexEntry?) : NioPageEntry {
    constructor(key: NioPageEntry, value: NioPageEntry) : this(key, ListPageEntry(mutableListOf(value)), null)

    var childPageNumber: Int? = null  // if not null points to a page containing bigger keys then this

    val isInnerNode
        get() = childPageNumber != null

    val isLeafNode
        get() = !isInnerNode

    override fun compareTo(other: NioPageEntry): Int {
        if (this === other) return 0
        if (javaClass != other.javaClass)
            return key.hashCode().compareTo(other.hashCode())
        other as NioBTreeEntry
        return key.compareTo(other.key)
    }

    // constructor(key: NioPageEntry) : this(key, null)
    override val length: Int
        get() = key.length + values.length + (if (childPageNumber != null) 4 else 0)
    override val type: NioPageEntryType
        get() = NioPageEntryType.ELSE

    override fun marshalTo(file: NioPageFile, offset: Long) {
        key.marshalTo(file, offset)
        values.marshalTo(file, offset + key.length)
        if (childPageNumber != null) {
            file.setInt(offset + key.length + values.length, childPageNumber ?: 0)
        } else {
            values.marshalTo(file, offset + key.length)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is NioBTreeEntry) return false

        if (key != other.key) return false
        if (values != other.values) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }

    fun addValue(value: NioPageEntry) {
        values.a.add(value)
    }

    fun removeValue(value: NioPageEntry) {
        values.a.remove(value)
    }

}

fun unmarshallEntry(page: NioPageFilePage, indexEntry: NioPageFilePage.IndexEntry): NioBTreeEntry {
    assert (!indexEntry.deleted)
    val offset = indexEntry.offsetInFile(page)
    var key = unmarshalFrom(page.file, offset)
    var values = unmarshalFrom(page.file, offset + key.length)
    val keyValueLen = key.length + values.length
    values as ListPageEntry
    val result = NioBTreeEntry(key, values, indexEntry)
    if (keyValueLen < indexEntry.len) {
        assert(indexEntry.len - keyValueLen == 4)
        var childpage = page.file.getInt(offset + keyValueLen)
        result.childPageNumber = childpage
    }
    return result
}

class NioBTree(val file: NioPageFile) {
    var root: NioPageFilePage? = null

    private fun insert(page: NioPageFilePage, toInsert: NioBTreeEntry): NioBTreeEntry? {
        val len = toInsert.length
        var pageEntries = getSortedEntries(page)

        val greater = pageEntries.find { it.compareTo(toInsert) >= 0 }
        val toInsertIndex = if (greater == null) pageEntries.size else pageEntries.indexOf(greater)
        if (greater != null && greater == toInsert) {
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
                    || greater == null && pageEntries.first().isLeafNode
                    || greater != null && greater.isLeafNode) {
                return insertAndSplitIfNecessary(page, toInsert, greater, toInsertIndex, pageEntries, false)
            } else {
                // found in inner Node
                assert (
                        greater == null && pageEntries.first().isInnerNode
                        || greater != null && greater.isInnerNode)
                assert( toInsertIndex > 0)  // leftmost element must be smaller than all
                if (toInsert.childPageNumber == null) {
                    // need to go until found position in leaf
                    val childPageNumber = pageEntries[toInsertIndex - 1].childPageNumber
                    if (childPageNumber == null)
                        throw AssertionError("expected childpagenumber to be != null in inner node")
                    else {
                        val nextLayerPage = NioPageFilePage(file, childPageNumber)
                        val result = insert(nextLayerPage, toInsert)
                        if (result != null) {
                            // insert this in current page since a split has occurred
                            val result = insert(page, result)
                            return result
                        }
                    }
                } else {
                    // insert into inner page
                    return insertAndSplitIfNecessary(page, toInsert, greater, toInsertIndex, pageEntries, true)
                }
            }
        return null
    }

    private fun getSortedEntries(page: NioPageFilePage): MutableList<NioBTreeEntry> {
        var pageEntries = mutableListOf<NioBTreeEntry>()
        page.indexEntries().forEach {
            if (!it.deleted)
                pageEntries.add(unmarshallEntry(page, it))
        }

        pageEntries.sort()
        return pageEntries
    }

    private fun insertAndSplitIfNecessary(page: NioPageFilePage,
                                          toInsert: NioBTreeEntry,
                                          greater: NioBTreeEntry?,
                                          toInsertIndex: Int,
                                          pageEntries: MutableList<NioBTreeEntry>,
                                          isInnerPage: Boolean): NioBTreeEntry? {
        if (page.allocationFitsIntoPage(toInsert.length)) {
            page.add(toInsert)
            return null
        }
        else {
            if (greater == null)
                pageEntries.add(toInsert)
            else
                pageEntries.add(toInsertIndex, toInsert)
            val completeLength = pageEntries.sumBy { it.length } + toInsert.length
            var currentSum = 0
            var splitEntry: NioBTreeEntry? = null

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
                            assert (e.childPageNumber != null)
                            val firstEntry = NioBTreeEntry(EmptyPageEntry(),EmptyPageEntry())
                            // save here childPageNumber of splitEntry, so before first element in page,
                            // all elements of page on which the splitEntry points are positions
                            firstEntry.childPageNumber = splitEntry.childPageNumber
                            newPage.add(firstEntry)
                        }
                        splitEntry.childPageNumber = newPage.number.toInt()
                    } else {
                        // then add entry to newpage, because it is right from the splitEntry
                        newPage.add(e)
                    }

                    if (!(e === toInsert) && e.indexEntry != null) {
                        // remove splitentry and right of limit, if not newly to be inserted (not yet in tree)
                        page.remove(e.indexEntry)
                    }
                } else {
                    if (e === toInsert)
                        insertLeft = true
                }
            }
            // entry to be inserted should be on the left s
            if (insertLeft) {
                // new entry not inserted in right page and not split-element
                page.add(toInsert)
            }
            return splitEntry
        }
    }

    fun getPrepareRoot() : NioPageFilePage {
        val result =
                if (!file.usedPagesIterator().hasNext()) {
                    val result = file.newPage()
                    val leaf = file.newPage()
                    val firstRootElement = NioBTreeEntry(EmptyPageEntry(), EmptyPageEntry())
                    firstRootElement.childPageNumber = leaf.number
                    result.add(firstRootElement)
                    file.rootPage = result.number
                    result
                } else {
                     NioPageFilePage(file, file.rootPage)
                }
        root = result
        return result
    }

    fun insert(tx: TXIdentifier, key: NioPageEntry, value: NioPageEntry) {
        val toInsert = NioBTreeEntry(key, value)

        val splitElement = insert(getPrepareRoot(), toInsert)
        if (splitElement != null) {
            val newRoot = file.newPage()
            val firstRootElement = NioBTreeEntry(EmptyPageEntry(), EmptyPageEntry())
            firstRootElement.childPageNumber = root!!.number
            newRoot.add(firstRootElement)
            newRoot.add(splitElement)
            file.rootPage = newRoot.number
            root = newRoot
        }
    }

    private fun delete(page: NioPageFilePage, toDelete: NioPageEntry, value: NioPageEntry, changedPages: MutableList<Int>): Unit {
        var pageEntries = getSortedEntries(page)
        val greater = pageEntries.find { it.compareTo(toDelete) >= 0 }
        val index = if (greater == null) pageEntries.size else pageEntries.indexOf(greater)
        if (greater != null && greater.compareTo(toDelete) != 0 || greater == null) {  // not found yet
            assert (index > 0)
            val nextChildPageNo = pageEntries[index - 1].childPageNumber
            if (nextChildPageNo == null) {
                throw IndexOutOfBoundsException("entry to be deleted not found in tree")
            }
            val child = NioPageFilePage(file, nextChildPageNo)
            delete(child, toDelete, value, changedPages)
            if (child.freeSpace() > (PAGESIZE.toInt()) * 2  / 3) {
                // try to remove child
                if (index > 1) {
                    // child is small, try to add it to the left
                    val prevChildPageNo = pageEntries[index-2].childPageNumber
                    val leftPage = if (prevChildPageNo == null) null else NioPageFilePage(file, prevChildPageNo)
                    if (leftPage == null)
                        throw AssertionError("expected left page to be available for merging")

                    // TODO: do exact calculation of: page fits in predecessor (use freeindexentries)
                    // leftPage.compactIndexArea()
                    // child.compactIndexArea()

                    if (leftPage.freeSpace() > (
                            PAGESIZE - child.freeSpace())) {
                        // should fit
                        for (e in child.indexEntries()) {
                            if (!e.deleted) {
                                val entry = unmarshallEntry(child, e)
                                if (leftPage.allocationFitsIntoPage(entry.length)) {
                                    leftPage.add(entry)
                                    child.remove(e)
                                } else {
                                    throw AssertionError("all should fit into left")
                                }
                            }
                        }
                        page.remove(pageEntries[index - 1].indexEntry!!)
                        file.freePage(child)
                    }
                } else {
                    // leftmost page is small, try to add everything from the right page
                    val rightIndexEntry = pageEntries[index].indexEntry
                    if (rightIndexEntry != null) {
                        val nextChildPageNo = pageEntries[index].childPageNumber
                        val rightPage = if (nextChildPageNo == null) null else NioPageFilePage(file, nextChildPageNo)
                        if (rightPage == null)
                            throw AssertionError("expected left page to be available for merging")
                        // TODO: do exact calculation of: page fits in predecessor (use freeindexentries)
                        // leftPage.compactIndexArea()
                        // child.compactIndexArea()

                        if (child.freeSpace() > (
                                PAGESIZE - rightPage.freeSpace())) {
                            // should fit
                            for (e in rightPage.indexEntries()) {
                                if (!e.deleted) {
                                    val entry = unmarshallEntry(rightPage, e)
                                    if (child.allocationFitsIntoPage(entry.length)) {
                                        child.add(entry)
                                        rightPage.remove(e)
                                    } else {
                                        throw AssertionError("all should fit into left")
                                    }
                                }
                            }
                            page.remove(rightIndexEntry)
                            // everything can stay as it was since the page, the rightIndexEntry pointed to, has been cleared
                            file.freePage(rightPage)
                        }
                    }
                }
            }
        } else {
            val greaterIndexEntry = greater.indexEntry
            if (greaterIndexEntry == null)
                throw AssertionError("sorted pageentries generated with indexentry null")
            val orgSize = greater.values.a.size
            if (value !is EmptyPageEntry)
                greater.values.a.remove(value)

            if (greater.values.a.size < orgSize || value is EmptyPageEntry) {
                if (greater.values.a.size == 0 || value is EmptyPageEntry) {
                    val nextChildPageNo = pageEntries[index].childPageNumber
                    if (nextChildPageNo == null) {
                        page.remove(greater.indexEntry)
                        changedPages.add(page.number)
                    } else {
                        val child = NioPageFilePage(file, nextChildPageNo)

                        var entryForReplacement = findSmallestEntry(child)
                        // now delete it from child
                        delete(child, entryForReplacement, EmptyPageEntry(), changedPages)
                        if (child.freeSpace() > (PAGESIZE.toInt()) * 2  / 3) {
                            assert(index > 0)
                            val prevChildPageNo = pageEntries[index-1].childPageNumber
                            val leftPage = if (prevChildPageNo == null) null else NioPageFilePage(file, prevChildPageNo)
                            if (leftPage == null)
                                throw AssertionError("expected left page to be available for merging")

                            // TODO: do exact calculation of: page fits in predecessor (use freeindexentries)
                            // leftPage.compactIndexArea()
                            // child.compactIndexArea()

                            if (leftPage.freeSpace() > (
                                    PAGESIZE + entryForReplacement.length + leftPage.INDEX_ENTRY_SIZE
                                            - child.freeSpace())) {
                                // should fit
                                leftPage.add(entryForReplacement)
                                for (e in child.indexEntries()) {
                                    if (!e.deleted) {
                                        val entry = unmarshallEntry(child, e)
                                        if (leftPage.allocationFitsIntoPage(entry.length)) {
                                            leftPage.add(entry)
                                            child.remove(e)
                                        } else {
                                            throw AssertionError("all should fit into left")
                                        }
                                    }
                                }
                            }
                            page.remove(greater.indexEntry)
                            file.freePage(child)
                        } else {
                            entryForReplacement.childPageNumber = greater.childPageNumber
                            page.remove(greater.indexEntry)
                            if (!page.allocationFitsIntoPage(entryForReplacement.length))
                            // TODO: implement if child entry does not fit into inner page during deletion
                                throw NotImplementedError("TODO: implement if child entry does not fit into inner page during deletion")
                            page.add(entryForReplacement)
                            changedPages.add(page.number)
                        }
                    }
                } else {  // remove this value, entry, linkage, ... stay except if merge is necessary
                    page.remove(greater.indexEntry)
                    page.add(greater)
                    changedPages.add(page.number)
                }
            } else {
                throw IndexOutOfBoundsException("value not found associated with key")
            }
        }
    }

    private fun findSmallestEntry(child: NioPageFilePage): NioBTreeEntry {
        val childIndexEntries = child.indexEntries()
        var e: NioPageFilePage.IndexEntry = getNextValidIndexEntry(childIndexEntries)
        var entryForReplacement = unmarshallEntry(child, e)
        if (entryForReplacement.isInnerNode) {
            assert(childIndexEntries.hasNext())
            entryForReplacement = unmarshallEntry(child, getNextValidIndexEntry(childIndexEntries))
        }
        return entryForReplacement
    }

    private fun getNextValidIndexEntry(childIndexEntries: Iterator<NioPageFilePage.IndexEntry>): NioPageFilePage.IndexEntry {
        while (childIndexEntries.hasNext()) {
            val e = childIndexEntries.next()
            if (!e.deleted)
                return e
        }

        throw AssertionError("expecting at least one valid entry")
    }

    fun remove(tx: TXIdentifier, key: NioPageEntry, value: NioPageEntry) {
        val changedPages = mutableListOf<Int>()

        delete(getPrepareRoot(), key, value, changedPages)
        // TODO: handle merging changed Pages if necessary
    }

    /*
    fun find(tx: TXIdentifier, entry: NioPageEntry) : Iterator<NioPageEntry> {

    }

    fun iterator(tx: TXIdentifier) : Iterator<NioPageEntry> {

    }
    */
}