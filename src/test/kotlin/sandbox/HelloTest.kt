package hello.tests

import hello.getHelloString
import kotlin.test.assertEquals
import org.junit.Test

class HelloTest {

    fun isOdd(a: Int) : Boolean = a % 2 == 1

    @Test fun testAssert() : Unit {
        assertEquals("Hello, world!", getHelloString())
        for (x in 1..5) {
            print(x)
        }

        val l = listOf(1,2,3,4,5,6)
        val grouped =  l.groupBy { it !in 1..20 step 2 }
        println(grouped.size)
        println(grouped[false])
        println(grouped[true])
    }
}
