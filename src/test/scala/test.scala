// package dataflat

import dataflat.IO._
import org.scalatest._

// TODO: need more comprehensive tests
class IOSpec extends FlatSpec with ShouldMatchers {

  val test_a: Map[String, List[(Long, Long)]] = List(
    ("chrA", List((123L, 100L), (456L, 200L))),
    ("chrB", List((123L, 150L), (456L, 250L))),
    ("chrC", List((123L, 200L), (456L, 300L)))
  ).toMap

  def parser(s: String): (Long, Long) = {
    // splitting with tab is obvious step, should be handled internally ? 
    val ss = s.split('\t')
    (ss(0).toLong, ss(1).toLong)
  }

  def deparser(t: (Long, Long)): String = s"${t._1}\t${t._2}"

  writeFlat("df.test.flt", test_a, deparser, List("# Hi"))

  "readFlat() and writeFlat()" should "be the inverse of each other." in {
    val read_a = readFlat("df.test.flt", parser)
    println(test_a)
    println(read_a)
    (read_a == test_a) should be (true)
  }

  "readFlatRefs()" should "read only the appropriate entries." in {
    val refs = List("chrB", "chrC")
    val read_a = readFlatRefs("df.test.flt", parser, refs)
    println(read_a)
    (read_a == test_a.filterKeys(k => refs.contains(k))) should be (true)
  }
}

/*
val test_b = List(
  ("chrD", List((123, 100), (456, 200))),
  ("chrE", List((123, 150), (456, 250))),
  ("chrF", List((123, 200), (456, 300)))
).toMap
*/
