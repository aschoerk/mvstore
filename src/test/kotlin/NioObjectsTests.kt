/**
 * @author aschoerk
 */

package nioobjects
import org.junit.Before
import org.junit.Test
import java.nio.IntBuffer
import java.util.*


class NioObjectBufferTest {
    val BUFFER_LENGTH = 100
    val RANDOM_START = BUFFER_LENGTH / 2
    val intBuffer: IntBuffer = IntBuffer.allocate(BUFFER_LENGTH)
    val intReference = IntArray(BUFFER_LENGTH)
    val rand = Random()

    @Before fun setup() {
        intBuffer.clear()
        for (i in 0 until RANDOM_START) {
            intBuffer.put(i, i)
        }
        for (i in RANDOM_START .. BUFFER_LENGTH-1) {
            val nextInt = rand.nextInt()
            intBuffer.put(i, nextInt)
            intReference[i] = nextInt
        }
    }

    @Test fun testInt() {


    }

}