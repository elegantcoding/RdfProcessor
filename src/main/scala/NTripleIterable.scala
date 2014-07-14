package com.elegantcoding.rdfprocessor

import java.io._
import rdftriple.{ValidRdfTriple, InvalidRdfTriple}
import rdftriple.types.RdfTriple

trait RdfTripleIterable extends Iterable[RdfTriple]{
  def iterator:Iterator[RdfTriple]
}

class NTripleIterable(is:InputStream, filter:(String)=>Boolean = (s:String) => true) extends RdfTripleIterable {
  val reader = new BufferedReader(new InputStreamReader(is), 8192*256)

  override def iterator:Iterator[RdfTriple] =
    Iterator.continually(reader.readLine)
      .takeWhile((s:String) => s != null)
      .withFilter(filter)
      .map(parseTriple(_))

  // this is significantly faster (half an order of magnitude) than split
  def parseTriple(rdfLine: String):RdfTriple = {
    if (rdfLine.trim().length() == 0)
      return InvalidRdfTriple()

    val idx = rdfLine.indexOf('\t')
    if (idx <= 0)
      return InvalidRdfTriple(rdfLine.trim())

    val first = rdfLine.substring(0, idx)
    val idx2 = rdfLine.indexOf('\t', idx + 1)
    if (idx2 <= 0)
      return InvalidRdfTriple(first,
        rdfLine.substring(idx + 1, rdfLine.length).trim())

    val second = rdfLine.substring(idx + 1, idx2)
    val idx3 = rdfLine.indexOf('\t', idx2 + 1)
    if (idx3 <= 0)
      return InvalidRdfTriple(first, second,
        rdfLine.substring(idx2 + 1, rdfLine.length).trim())
    val third = rdfLine.substring(idx2 + 1, idx3)
    ValidRdfTriple(first, second, third)
  }
}