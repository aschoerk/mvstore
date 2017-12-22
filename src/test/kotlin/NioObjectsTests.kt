/**
 * @author aschoerk
 */

package nioobjects
import org.junit.Before
import org.junit.Test
import java.nio.IntBuffer
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class NioObjectBufferTest {
    val BUFFER_LENGTH = 100
    val RANDOM_START = BUFFER_LENGTH / 2
    val intBuffer: IntBuffer = IntBuffer.allocate(BUFFER_LENGTH)
    val intReference = IntArray(BUFFER_LENGTH)
    val rand = Random()

    @Before fun setup() {
        intBuffer.clear()
        for (i in 0..RANDOM_START - 1) {
            intBuffer.put(i, i)
        }
        for (i in RANDOM_START .. BUFFER_LENGTH-1) {
            val nextInt = rand.nextInt()
            intBuffer.put(i, nextInt)
            intReference[i] = nextInt
        }
    }

    @Test fun testInt() {
        val b = NioObjectBuffer(intBuffer,1)
        assertEquals(1, b.getInt())
        assertEquals(b.idx, 2)
        assertEquals(2, b.getInt())
        assertEquals(b.idx, 3)
        assertEquals(3, b.getByte(16))
        assertEquals(b.idx, 5)
        assertEquals(0, b.getByte(17))
        assertEquals(b.idx, 5)
        assertTrue { b.getBoolean(16) }
        assertEquals(b.idx, 5)
        assertFalse { b.getBoolean(17) }
        assertEquals(b.idx, 5)

    }

}