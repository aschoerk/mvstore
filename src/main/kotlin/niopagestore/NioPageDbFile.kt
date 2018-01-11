package niopagestore

import nioobjects.TXIdentifier
import niopageentries.*
import niopageobjects.INioPageFile
import niopageobjects.MVCCFile
import niopageobjects.NioPageFile

const val PAGE_DB_MAGIC = -0x12568762
const val PAGE_DB_MAGIC_OFFSET = 4L

const val DIR_ROOT_PAGE_OFFSET = 32L

class NioBTreeDirEntry(val rootPage: Int, val mvcc: Boolean)
    : ListPageEntry(listOf<NioPageEntry>(IntPageEntry(rootPage), BooleanPageEntry(mvcc))) {

}

class NioPageDbFile(val file: NioPageFile) {
    init {
        val magic = file.getInt(PAGE_DB_MAGIC_OFFSET)
        if (magic != PAGE_DB_MAGIC) {
            file.initFreeSpaceSafely()

            file.setInt(PAGE_DB_MAGIC_OFFSET, PAGE_DB_MAGIC)
            val rootPage = file.newPage()
            file.setInt(DIR_ROOT_PAGE_OFFSET, rootPage.number)
        } else {
            file.checkFreeSpace()
            assert (file.getInt(DIR_ROOT_PAGE_OFFSET) != 0)
        }
    }

    val directory
        get() = NioBTree(file, file.getInt(DIR_ROOT_PAGE_OFFSET))

    fun createBTree(name: String) = createBTree(name, true)

    fun createBTree(name: String, mvcc: Boolean) : NioBTree {
        val key = StringPageEntry(name)
        assert (directory.find(key) == null)
        val root = file.newPage()
        directory.insert(TXIdentifier(), key, NioBTreeDirEntry(root.number, mvcc))
        if (mvcc)
            return NioBTree(MVCCFile(file), root.number)
        else
            return NioBTree(file, root.number)
    }

    fun getBTree(name: String) : NioBTree {
        val key = StringPageEntry(name)
        val result = directory.findSingle(key)
        result as ListPageEntry
        val rootNumber = (result[0] as IntPageEntry).v
        val mvcc = (result[1] as BooleanPageEntry).v
        if (mvcc)
            return NioBTree(MVCCFile(file), rootNumber)
            else
        return NioBTree(file, rootNumber)
    }

}