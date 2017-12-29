package niopagestore

import nioobjects.TXIdentifier
import niopageobjects.NioPageFile

class NioBTree(val file: NioPageFile, val keyType: NioPageEntryType) {
    fun insert(tx: TXIdentifier, entry: NioPageEntry) {

    }

    fun remove(tx: TXIdentifier, entry: NioPageEntry) {

    }

    /*
    fun find(tx: TXIdentifier, entry: NioPageEntry) : Iterator<NioPageEntry> {

    }

    fun iterator(tx: TXIdentifier) : Iterator<NioPageEntry> {

    }
    */
}