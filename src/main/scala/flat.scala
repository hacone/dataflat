package dataflat

object IO {

// TODO: handle exception (open error etc)
def writeFlat[T](filename: String, data: Map[String, List[T]],
  deparser: T => String, header: List[String] = List.empty[String]): Unit = {
    
  assert(header.forall(_.startsWith("#")))

  var idx: Map[String, Long] = Map.empty
  var byteWritten: Long = 0
  val pw = new java.io.PrintWriter(new java.io.File(filename))

  // write header
  header.foreach(pw.println(_))
  byteWritten = header.map(_.length + 1).sum

  // write data
  for (k <- data.keys.toList.sorted) {
    idx += ((k, byteWritten))
    pw.println(s">${k}")
    byteWritten += s">${k}".length + 1
    for(t <- data(k)) {
      val l = deparser(t)
      pw.println(l)
      byteWritten += l.length + 1
    }
  }

  pw.close

  // write index file
  val pwi = new java.io.PrintWriter(new java.io.File(filename + ".idx"))
  for (k <- idx.keys.toList.sorted) pwi.println(s"${k}\t${idx(k)}")
  pwi.close
}

def readFlat[T](filename: String, parser: String => (Long, T)): Map[String, List[(Long, T)]] = {
  var acc = scala.collection.mutable.ListBuffer.empty[(String, List[(Long, T)])]
  var ref = ""
  var lb = scala.collection.mutable.ListBuffer.empty[(Long, T)]

  for (l <- scala.io.Source.fromFile(filename).getLines; if ! l.startsWith("#")) {
    if (l.startsWith(">")) {
      if (ref != "") {
        acc += ((ref, lb.result))
        // initialize vars
        lb = scala.collection.mutable.ListBuffer.empty[(Long, T)]
        ref = l.tail
      } else {
        ref = l.tail
      }
    } else {
      lb += parser(l)
    }
  }
  if (ref != "") acc += ((ref, lb.result))
  acc.toMap
}

def readFlatRefs[T](filename: String, parser: String => (Long, T), refs: List[String]): Map[String, List[(Long, T)]] = {
  val offsets: Map[String, Long] = scala.io.Source.fromFile(filename + ".idx")
    .getLines.toList.map { l => val s = l.split('\t'); (s(0), s(1).toLong) }.toMap
  val file = new java.io.RandomAccessFile(filename, "r")
  var acc = scala.collection.mutable.ListBuffer.empty[(String, List[(Long, T)])]

  for (r <- refs) {
    assert(offsets.contains(r))
    val lb = scala.collection.mutable.ListBuffer.empty[(Long, T)]

    // another assertion: TODO: this should be in the test
    file.seek(offsets(r))
    assert( file.readByte().toChar == '>' )
    assert( file.readLine() == r )

    file.seek(offsets(r))
    file.readLine()

    var break = false
    while (!break && file.getFilePointer < file.length) {
      val l = file.readLine
      if (l.startsWith(">")) { break = true }
      else if (! l.startsWith("#")) { lb += (parser(l)) }
    }

    acc += ((r, lb.result))
  }
  file.close
  acc.toMap
}

/* // TODO: need interface like these

// T should be able to call toString
def writeFlat[T](data: Map[String, List[(Int, T)]], header: List[String]): Unit = {
  def toRecord(x: (Int, T)): String = s"${x._1}\t${x._2.toString}"
  writeFlat(data.mapValues(_.map(toRecord)))
}


def writeFlat[T, S](data: Map[String, List[(Int, T, S)]], header: List[String]): Unit = {
  def toRecord(x: (Int, T, S)): String = s"${x._1}\t${x._2.toString}\t${x._3.toString}"
  writeFlat(data.)
}

*/

}
