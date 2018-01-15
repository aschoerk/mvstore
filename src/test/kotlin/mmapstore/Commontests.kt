package mmapstore

import org.junit.Test
/**
 * @author aschoerk
 */
class Tests {
    @Test
    fun test() {
        var counter = 0
        (1..1000).asSequence().filter{ println("$it"); it % 2 == 1}.forEach{
            counter++
            if (counter > 10)
                return
        }

    }
}