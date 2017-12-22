/**
 * @author aschoerk
 */
package pg

import nioobjects.NioBufferWithOffset
import nioobjects.NioSizedObject
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.IntBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import kotlin.test.assertTrue

val RELKIND_RELATION = 'r'
val RELKIND_INDEX = 'i'
val RELKIND_SEQUENCE = 'S'
val RELKIND_TOASTVALUE = 't'

val RELKIND_VIEW = 'v'
val RELKIND_COMPOSITE_TYPE = 'c'
val RELKIND_FOREIGN_TABLE = 'f'
val RELKIND_MATVIEW = 'm'

val BLCKSZ = 8192

// ???

class TransactionId(val id: Int)

// relfilenode.h

class RelFileNode(val spcNode: Oid, val dbNode: Oid, val relNode: Oid)

class SMgrRelationDataP(val p: Int)

class BackendId(val id: Int)

class LockRelId(val relId: Oid, val dbId: Oid)

class LockInfoData(val relId: Oid, val dbId: Oid) {
    init {
        val lockRelId = LockRelId(relId, dbId)
    }
}

class NameData(var b: ByteBuffer, var index: Int) {
    var name: String
    init {
        var sb = StringBuilder()
        for (i in 0..63) {
            val i = b[index+i]
            if (i == 0.toByte())
                break;
            sb.append(i.toChar())
        }
        name = sb.toString()
    }
}

class SubTransactionId(val id: Int)

val INVALID_BLOCK_NUMBER = 0xFFFF_FFFF.toInt()

val MAX_BLOCK_NUMBER = 0xFFFF_FFFE.toInt()


// buf_internals.h

class BufferTag(val rnode: RelFileNode, val forkNum: ForkNumber, val blockNum: BlockNumber)

class PgAtomicInt(val int: Int)

class LwLock

class BufferDesc(val tag: BufferTag,
                 val id: Int,
                 val state: PgAtomicInt,
                 val waitBackendPid: Int,
                 val freeNext: Int,
                 val contentLock: LwLock)


// buffer.h

class BlockIdData(val hi: Short, val lo: Short) {
    constructor(blockNumber: BlockNumber)
            : this((blockNumber.number shr 16).toShort(), (blockNumber.number and 0xFFFF).toShort())

    fun toBlockNumber(): BlockNumber
            = BlockNumber(hi.toInt() shl 16 or lo.toInt())
}

class BlockNumber(val number: Int) {
    fun isValid() = number != INVALID_BLOCK_NUMBER

    fun toBlockId() = BlockIdData(this)
}



// postgres_ext.h

class Oid(val id: Int)

val INVALID_OID = Oid(0)

val OID_MAX = Oid(-1)

// relpath.h

enum class ForkNumber(val forkNumber: Byte)  {
    InvalidForkNumber(-1),
    MAIN_FORKNUM(0),
    FSM_FORKNUM(1),
    VISIBILITYMAP_FORKNUM(2),
    INIT_FORKNUM(3)
}

val MAX_FORK_NUMBER = ForkNumber.INIT_FORKNUM

val FORK_NAME_CHARS = 4

val RECORDOID = 2249

class RelationData(val bs: List<NioSizedObject>) : NioBufferWithOffset(bs[0]) {
    var len = bs[0].len
    var node
        get() = RelFileNode(Oid(getInt(0)), Oid(getInt(4)), Oid(getInt(8)))
        set(value) {
            setInt(0, value.spcNode.id)
            setInt(4, value.dbNode.id)
            setInt(8, value.relNode.id)
        }
    var refcnt
        get() = getInt(0x18)
        set(value) = setInt(0x18, value)
    var backend
        get() = BackendId(getInt(0x1c))
        set(value) = setInt(0x1c, value.id)
    var isLocalTemp
        get() = getBoolean(0x20)
        set(value) = setBoolean(0x20, value)
    var isNailed
        get() = getBoolean(0x21)
        set(value) = setBoolean(0x21, value)
    var isValid
        get() = getBoolean(0x22)
        set(value) = setBoolean(0x22, value)
    var indexValid
        get() = getByte(0x23)
        set(value) = setByte(0x23, value)
    var createSubid
        get() = SubTransactionId(getInt(0x24))
        set(value) = setInt(0x24, value.id)
    var newRelfilenodeSubid
        get() = SubTransactionId(getInt(0x25))
        set(value) = setInt(0x25, value.id)
    val rel
        get() = FormDataPgClass(bs[1])

    val att: TupleDesc
        get() {
            val relTmp = rel
            val result = TupleDesc(relTmp.nAtts, relTmp.hasOids)
            result.refCount = 1
            result.typeOid = rel.type
            result.typeMod = -1
            var hasNotNull = false
            for (i in 1..relTmp.nAtts) {
                val attr = FormDataPgAttribute(bs[1+i])
                result.attrs.add(i-1, attr)
                hasNotNull = hasNotNull or attr.notNull
            }
            if (hasNotNull) result.constr = TupleConstr(hasNotNull)
            return result
        }
    val options: NioSizedObject?
        get() {
            var len = getLong(att.behind)
            if (len == 0L)
                return null;
            else
                return bs[1+rel.nAtts]
        }
    val indextuple: HeapTuple?
        get() {
            if (rel.kind == RELKIND_INDEX) {
                bs[2+rel.nAtts]
            }
            return null
        }
}

class HeapTuple(b: NioBufferWithOffset) : NioSizedObject(b, -1)

class TupleConstr(val hasNotNull: Boolean)

class TupleDesc(val nAttrsP: Short, val hasoid: Boolean) {
    val nAttrs = nAttrsP.toInt()
    var attrs: MutableList<FormDataPgAttribute> = arrayListOf()
    var typeOid = Oid(RECORDOID)
    var typeMod = -1
    var hasOid = hasoid
    var refCount = -1
    var constr: TupleConstr? = null;
    var behind = -1
}

class FormDataPgClass(b: NioSizedObject) : NioBufferWithOffset(b) {
    val name
        get() = NameData(b, offset)
    val nAtts
        get() = getShort(0x70)
    val hasOids
        get() = getBoolean(0x74)
    val type
        get() = Oid(getInt(0x44))
    val kind
        get() = getChar(0x6f)
}

class FormDataPgAttribute(b: NioSizedObject) : NioBufferWithOffset(b) {
    val name
        get() = NameData(b, offset+4)
    val notNull
        get() = getBoolean(0x5f)
}

val RELATION_SIZE = 0x150L
val FORM_SIZE = 0x88L

fun interpretPgCachePart(part: List<NioSizedObject>) {
    assertTrue(part[0].len == RELATION_SIZE)
    assertTrue(part[1].len == FORM_SIZE)


}


fun main(args: Array<String>) {
    val filename = "/home/aschoerk/projects/mvcc/17178/pg_internal.init";
    val buffer = createMappedByteBuffer(filename)

    // val relData = RelationData(NioBufferPart(buffer,4))

    // assertTrue { relData.att.nAttrs > 0 }

    val sizedObjects: MutableList<NioSizedObject> = mutableListOf()
    val relations: MutableList<RelationData> = mutableListOf()
    var index = 4
    var currentBuffer = NioBufferWithOffset(buffer,4)
    try {
        do {
            val act = NioSizedObject(currentBuffer, -1)
            currentBuffer = act.bufferPartBehind()
            if (sizedObjects.size > 0 && act.len == RELATION_SIZE) {
                val rel = RelationData(sizedObjects)
                println("Relname: " + rel.rel.name.name + " relNode: " + rel.node.relNode.id)
                for (i in 1..rel.att.nAttrs) {
                    println("Attr   : " + rel.att.attrs[i-1].name.name)
                }
                relations.add(rel)
                sizedObjects.clear()
            }
            sizedObjects.add(act)
        } while (true)

    } catch (e: Exception) {
        println( e)
    }
    println( "done")

}

private fun createMappedByteBuffer(filename: String): ByteBuffer {
    val f = RandomAccessFile(filename, "rw");
    val channel = f.channel

    val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, f.length());

    buffer.order(ByteOrder.LITTLE_ENDIAN)
    return buffer
}