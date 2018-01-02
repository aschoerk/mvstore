package niopagestore

import nioobjects.TXIdentifier
import niopageentries.*
import niopageobjects.NioPageFile
import niopageobjects.NioPageFilePage


class NioBTreeEntry(val key: NioPageEntry, val value: NioPageEntry, val indexEntry: NioPageFilePage.IndexEntry?) : NioPageEntry {
    constructor(key: NioPageEntry, value: NioPageEntry) : this(key, value, null)

    val values: ListPageEntry = ListPageEntry(mutableListOf(value))
    var childPageNumber: Int? = null

    val isInnerNode
        get() = childPageNumber != null

    val isLeafNode
        get() = !isInnerNode

    override fun compareTo(other: NioPageEntry): Int {
        if (this === other) return 0
        if (javaClass != other.javaClass) return hashCode().compareTo(other.hashCode())
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

    val offset = indexEntry.offsetInFile(page)
    var key = unmarshalFrom(page.file, offset)
    var value = unmarshalFrom(page.file, offset + key.length)
    val keyValueLen = key.length + value.length
    val result = NioBTreeEntry(key, value, indexEntry)
    if (keyValueLen < indexEntry.len) {
        assert(indexEntry.len - keyValueLen == 4)
        var childpage = page.file.getInt(page.offset + keyValueLen)
        result.childPageNumber = childpage
    }
    return result
}

class NioBTree(val file: NioPageFile, val keyType: NioPageEntryType) {
    var root: NioPageFilePage? = null

    private fun insert(page: NioPageFilePage, toInsert: NioBTreeEntry, value: NioPageEntry): NioBTreeEntry? {
        val len = toInsert.length
        var pageEntries = getSortedEntries(page)

        val greater = pageEntries.find { it.compareTo(toInsert) >= 0 }
        val toInsertIndex = if (greater == null) pageEntries.size else pageEntries.indexOf(greater)
        if (greater != null && greater == toInsert) {
            // the key is found in the current page, multiples are allowed, so add the value to the BTreeEntryValues-List
            if (page.allocationFitsIntoPage(toInsert.value.length)) {
                greater.addValue(value)
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
                        val result = insert(nextLayerPage, toInsert, value)
                        if (result != null) {
                            val result = insert(page, result, EmptyPageEntry())
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
            val right = mutableListOf<NioBTreeEntry>()
            var splitEntry: NioBTreeEntry? = null

            var splitIndex = 0

            val newPage = file.newPage()
            for (e in pageEntries.iterator()) {
                currentSum += e.length
                if (currentSum >= completeLength / 2) {
                    if (splitEntry == null) {
                        // this entry will be returned to be inserted into the parent page
                        splitEntry = e
                        if (isInnerPage) {
                            assert (e.childPageNumber != null)
                            val firstEntry = NioBTreeEntry(EmptyPageEntry(),EmptyPageEntry())
                            firstEntry.childPageNumber = e.childPageNumber
                            newPage.add(firstEntry)
                        }
                        e.childPageNumber = newPage.number.toInt()
                    } else {
                        newPage.add(e)
                    }
                    if (e.indexEntry != null)
                        page.remove(e.indexEntry)
                } else {
                    splitIndex++
                }
            }
            if (toInsertIndex < splitIndex) {
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
                    result
                } else {
                     file.usedPagesIterator().next()
                }
        root = result
        return result
    }

    fun insert(tx: TXIdentifier, key: NioPageEntry, value: NioPageEntry) {
        val toInsert = NioBTreeEntry(key, value)

        insert(getPrepareRoot(), toInsert, value)
    }

    private fun delete(page: NioPageFilePage, toDelete: NioBTreeEntry, value: NioPageEntry): NioBTreeEntry? {
        val len = toDelete.length
        var pageEntries = getSortedEntries(page)

    }

        fun remove(tx: TXIdentifier, key: NioPageEntry, value: NioPageEntry) {
        val toDelete = NioBTreeEntry(key, value)

        delete(getPrepareRoot(), toDelete, value)
    }

    /*
    fun find(tx: TXIdentifier, entry: NioPageEntry) : Iterator<NioPageEntry> {

    }

    fun iterator(tx: TXIdentifier) : Iterator<NioPageEntry> {

    }
    */
}