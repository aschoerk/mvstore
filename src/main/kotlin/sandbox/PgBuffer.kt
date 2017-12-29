package sandbox

import pg.*
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.IntBuffer
import java.nio.channels.FileChannel
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * @author aschoerk
 */



// utils/rel.h

/*

class RelationData(
        val node: RelFileNode,
        val smgr: SMgrRelationDataP,
        val refcnt: Int,
        val backend: BackendId,
        val isLocalTemp: Boolean,
        val isNailed: Boolean,
        val isValid: Boolean,
        val indexValid: Char,
        val createSubId: SubTransactionId,
        val newRelfilenodeSubId: SubTransactionId,
        val rel: FormPgClass,
        val att: TupleDesc,
        val id: Oid,
        val lockInfo: LockInfoData,
        val rules: Array<RuleLock>,
        val rulesctx: MemoryContext,
        val trigDesc: TriggerDesc,
        val rsDesc: RowSecurityDesc,
        val fkeyList: List<ForeignKeyCacheInfo>,
        val fkeyValid: Boolean,
        val indexList: List<Index>,
        val oidIndex: Oid,
        val replidIndex: Oid,
        val indexAttr: BitmapSet,
        val keyAttr: BitmapSet,
        val idAttr: BitmapSet,

        val options: ByteA,
        val index: FormPgIndex,
        val indexTuple: Array<HeapTupleData>,
        val amHandler: Oid,
        val indexCtxt: MemoryContex,
        val amRoutine: IndexAmRoutine,
        val opFamily: Oid,
        val opCinType: Oid,
        val support: RegProcedure,
        val supportInfo: FmgrInfo,
        val indOption: Short,
        val indexPrs: List<IndexExpression>,
        val indPred: List<IndexPredicate>,
        val exclOps: Oid,
        val exclProcs: Oid,
        val exclStrats: Short,
        val amCache: Pointer,
        val indCollection: Oid,
        val fdwRoutine: FdwRoutine,
        val toastOid: Oid,
        val pgstatInfo: PgStatTableData
        )

        */

class RelationData(val b: IntBuffer, var index: Int) {
    init {
    }
        var node = RelFileNode(Oid(b[index++]), Oid(b[index++]), Oid(b[index++]))  // 0x0000
    init {
        index++; // padding
        index++; index++; // SMgrRelationDataP // 0x0010
    }
        var refcnt = b[index++]  // 0x0018
        var backend = BackendId(b[index++])  // 0x001c
        var islocaltemp = b[index] and 0xff != 0  // 0x0020
        var isnailed = b[index] and 0xff00 != 0   // 0x0021
        var isvalid = b[index] and 0xff0000 != 0  // 0x0022
        var indexvalid: Byte = (b[index++].toLong() and 0xff000000 shr 24).toByte()  // 0x0023
        var createSubId = SubTransactionId(b[index++]) // 0x0024
        var newRelfilenodeSubid = SubTransactionId(b[index++]) // 0x0028
    init {
        index++; // padding
        index++; index++; // Form_pg_class rel // 0x0030
        index++; index++; // TupleDesc att // 0x0038
    }
        var id = Oid(b[index++]) // 0x0040
        var lockInfo = LockInfoData(Oid(b[index++]), Oid(b[index++]))  // 0x0044
    init {
        index++; // padding
        index++; index++; // Rules  // 0x50
        index++; index++; // var rulesCtx = MemoryContext(b)  // 0x58
        index++; index++;  // TriggerDesc  // 0x60
        index++; index++;  // RowSecurityDesc  // 0x68
        index++; index++;  // rd_fkeylist  // 0x70
    }
        var fkeyValid = b[index++] and 0xff != 0  // 0x0078
    init {
        index++; // padding
        index++; index++; // indexlist  // 0x0080
    }
        var oidIndex = Oid(b[index++]) // 0x0088
        var replIdIndex = Oid(b[index++]) // 0x008c
    init {
        index++; index++; // indexattr  // 0x0090
        index++; index++;  // keyattr  // 0x0098
        index++; index++;  // idattr  // 0x00a0
        index++; index++;  // rd_options  // 0x00a8
        index++; index++;  // index  // 0x00b0
        index++; index++;  // indextuple  // 0x00b8
    }
        var oidAmHandler = Oid(b[index++]) // 0x00c0
    init {
        index++; // padding
        index++; index++;  // indexcxt  // 0x00c8
        index++; index++;  // amroutine  // 0x00d0
        index++; index++;  // opfamily  // 0x00d8
        index++; index++;  // opcintype  // 0x00e0
        index++; index++;  // support  // 0x00e8
        index++; index++;  // supportInfo  // 0x00f0
        index++; index++;  // indoption // 0x00f8
        index++; index++;  // indexprs // 0x0100
        index++; index++;  // indpred // 0x0108
        index++; index++;  // exclops // 0x0110
        index++; index++;  // exclprocs // 0x0118
        index++; index++;  // exclstrats // 0x0120
        index++; index++;  // amcache // 0x0128
        index++; index++;  // indcollation // 0x0130
        index++; index++;  // fdwroutine // 0x0138
    }
    var toastoid = Oid(b[index++]) // 0x0140
    init {
        index++; // padding
        index++; index++;  // pgstatinfo // 0x0148
        // assertTrue { index == indexP + 0x150 / 4 }
    }
    init {
        val formPgClassLen = b[index++]
        assert ( b[index++] == 0 )  // ignore upper bits
        assert( formPgClassLen == 136 )
    }
    val rel = FormDataPgClass(b, index)
    init {
        index += 34
    }
    val attr = TupleDesc(rel.relNAtts, rel.relHasOids)
    init {
        attr.tdRefCount = 1
        attr.tdTypeOid = rel.reltype
        attr.tdTypeMod = -1
    }
    init {
        for (i in 1..rel.relNAtts) {
            val attrLen = b[index++]
            assert( b[index++] == 0 )  // ignore upper bits
            assert( attrLen == 108 )
            attr.attrs.add(FormDataPgAttribute(b,index))
            index += 27

        }
    }
}

class FormDataPgAttribute(var b: IntBuffer, var index: Int) {
    val relId = Oid(b[index++])
    val name = NameData(b, index)
    init {
        index += 16
    }
    val typeId = Oid(b[index++])
    val stattarget = b[index++]
    val len = (b[index] and 0xFFFF).toShort()
    val num = ((b[index++].toLong() and 0xFFFF0000) shr 16).toShort()
    val nDims = b[index++]
    val cacheOffs = b[index++]
    val typMod = b[index++]
    val byVal = b[index] and 0xff != 0
    val storage = (b[index] and 0xff00) shr 8  // 0x5d
    val align = (b[index] and 0xff0000) shr 16  // 0x5e
    val notNull = (b[index++].toLong() and 0xff000000) != 0L  // 0x5f
    val hasDef = b[index] and 0xff != 0  // 0x60
    val isDropped = (b[index] and 0xff00) != 0   // 0x61
    val isLocal = (b[index++] and 0xff0000) != 0  // 0x62
    val inhCount = b[index++]  // 0x64
    val collation = Oid(b[index])  // 0x68


}

class NameData(var b: IntBuffer, var index: Int) {

    var name: String
    init {
        var sb = StringBuilder()
        for (i in 0..15) {
            val i = b[index+i]
            sb.append((i and 0xFF).toChar())
            sb.append(((i and 0xFF00) shr 8).toChar())
            sb.append(((i and 0xFF0000) shr 16).toChar())
            sb.append(((i.toLong() and 0xFF000000) shr 24).toChar())
        }
        name = sb.toString()
    }
}


class FormDataPgClass(val b: IntBuffer, val indexP: Int) {
        var index = indexP
        val relname = NameData(b, index)
    init {
        index += 16
    }
        val relnamespace = Oid(b[index++])
        val reltype = Oid(b[index++])
        val reloftype = Oid(b[index++])
        val relowner = Oid(b[index++])
        val relam = Oid(b[index++])
        val relfilenode = Oid(b[index++])
        val reltablespace = Oid(b[index++])
        val relpages = b[index++]
        val reltuples = java.lang.Float.intBitsToFloat(b[index++])
        val relallvisible = b[index++]
        val reltoastrelid = Oid(b[index++])
        var relHasIndex = b[index] and 0xff != 0
        var relIsShared = b[index] and 0xff00 != 0
        var relPersistence = (b[index] and 0xff0000 shr 16).toByte()
        var relKind: Byte = (b[index++].toLong() and 0xff000000 shr 24).toByte()
        var relNAtts = (b[index] and 0xffff).toShort()
        var relChecks = (b[index++].toLong() and 0xffff0000 shr 16).toShort()
        var relHasOids = b[index] and 0xff != 0
        var relHasPKey = b[index] and 0xff00 != 0
        var relHasRules = b[index] and 0xff0000 != 0
        var relHasTriggers = b[index++].toLong() and 0xff000000 != 0L
        var relHasSubClass = b[index] and 0xff != 0
        var relRowSecurity = b[index] and 0xff00 != 0
        var relForceRowSecurity = b[index] and 0xff0000 != 0
        var relIsPopulated = b[index++].toLong() and 0xff000000 != 0L
        var relReplIdent = b[index++] and 0xff
        var relFrozenXid = TransactionId(b[index++])
        var relMinMXid = TransactionId(b[index++])
    init {
        assert( index == indexP + 34 )
    }
}



// bufpage.h

class LocationIndex(val index: Short)

class PageXLogRecPtr(val ptr: Long)

class ItemIdData(val off: Short, val flags: Byte, val len: Short)

class PageHeaderData(val lsn: PageXLogRecPtr,
                     val checksum: Short,
                     val flags: Short,
                     val lower: LocationIndex,
                     val upper: LocationIndex,
                     val special: LocationIndex,
                     val pageSizeVersion: Short,
                     val pruneXid: TransactionId,
                     val lin: Array<ItemIdData>
                     )

class TupleDesc(val nAttrsP: Short, val hasoid: Boolean) {
    val nAttrs = nAttrsP.toInt()
    var attrs: MutableList<FormDataPgAttribute> = arrayListOf()
    var tdTypeOid = Oid(RECORDOID)
    var tdTypeMod = -1
    var tdHasOid = hasoid
    var tdRefCount = -1
    var constr: TupleConstr? = null;
}


fun main(args: Array<String>) {

    val filename = "/home/aschoerk/projects/mvcc/17178/pg_internal.init";
    val f = RandomAccessFile(filename,"rw");
    val channel = f.channel

    val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, f.length());

    buffer.order(ByteOrder.LITTLE_ENDIAN)

    val intbuffer = buffer.asIntBuffer();

    val maxInt = f.length() / 4
    val RELCACHE_INIT_FILEMAGIC = 0x573266
    assert(
        intbuffer[0] == RELCACHE_INIT_FILEMAGIC
    )

    var currentIndex = 1;

    while (currentIndex < maxInt) {
        val descriptorLen = intbuffer[currentIndex++]
        assert(intbuffer[currentIndex++] == 0)  // ignore upper bits
        assert( descriptorLen == 0x150 )
        assert(currentIndex + 84 <= maxInt)
        val relation = RelationData(intbuffer, currentIndex)
        currentIndex += 84
       




    }

}

