package niopagestore

import niopageobjects.*
import org.junit.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * @author aschoerk
 */

class Test {
    val freeSpageRegionPageNum = (8192 - 4 /* magic */) * 8 + 1
    val freeSpageRegionPageNumByteLength = freeSpageRegionPageNum * 8192

    @Test
    fun freespaceRegionTest() {

        assertTrue(freespaceRegion(0) == 0)
        assertTrue(freespaceRegion(1) == 0)

        assertTrue(freespaceRegion(freeSpageRegionPageNum) == 0)
        // yet in reqion because of page 0
        assertTrue(freespaceRegion(2 * freeSpageRegionPageNum) == 1)
        // now out of first region
        assertTrue(freespaceRegion(2 * freeSpageRegionPageNum + 1) == 2)
        // yet in second reqion because of page 0
        assertTrue(freespaceRegion(3 * freeSpageRegionPageNum) == 2)
        assertTrue(freespaceRegion(3 * freeSpageRegionPageNum + 1) == 3)

        assertTrue(freespaceRegionOffset(1) == 8192L)
        assertTrue(freespaceRegionOffset(freeSpageRegionPageNum) == 8192L)
        assertTrue(freespaceRegionOffset(2 * freeSpageRegionPageNum) ==
                8192L + freeSpageRegionPageNum * 8192L)

        assertTrue(freespaceRegionOffset(freeSpageRegionPageNum) == 8192L)
        // yet in reqion because of page 0
        assertTrue(freespaceRegionOffset(2 * freeSpageRegionPageNum)
                == freespaceRegionOffset(freeSpageRegionPageNum) + freeSpageRegionPageNumByteLength)
        assertTrue(freespaceRegionOffset(2 * freeSpageRegionPageNum + 1)
                == freespaceRegionOffset(2 * freeSpageRegionPageNum) + freeSpageRegionPageNumByteLength)

    }

    @Test
    fun freespaceByteOffsetTest() {
        try{
            freespaceByteOffset(1) == 1L
            fail("expected assertion exception")
        }
        catch (ex: AssertionError) {

        }
        try{
            freespaceByteOffset(1 + freeSpageRegionPageNum) == 1L
            fail("expected assertion exception")
        }
        catch (ex: AssertionError) {

        }
        assertTrue(freespaceByteOffset(2) == 4L)
        assertTrue(freespaceByteOffset(9) == 4L)
        assertTrue(freespaceByteOffset(10) == 5L)
        assertTrue(freespaceByteOffset(freeSpageRegionPageNum) == 8191L)
        assertTrue(freespaceByteOffset(2 + freeSpageRegionPageNum) == 4L)
        assertTrue(freespaceByteOffset(9 + freeSpageRegionPageNum) == 4L)
        assertTrue(freespaceByteOffset(10 + freeSpageRegionPageNum) == 5L)
        assertTrue(freespaceByteMask(2) == 1)
        assertTrue(freespaceByteMask(9) == 0x80)
        assertTrue(freespaceByteMask(10) == 1)
        assertTrue(freespaceByteMask(2 + freeSpageRegionPageNum) == 1)
        assertTrue(freespaceByteMask(9 + freeSpageRegionPageNum) == 0x80)
        assertTrue(freespaceByteMask(10 + freeSpageRegionPageNum) == 1)
    }
}
