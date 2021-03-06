package com.elegantcoding.rdfprocessor

import java.io._
import scala.language.existentials
import com.elegantcoding.rdfprocessor.rdftriple.{ValidRdfQuadruple, ValidRdfTriple, InvalidRdfTriple}
import com.elegantcoding.rdfprocessor.rdftriple.types.{RdfQuadruple, RdfTriple, LineFilter, RdfTupleFilter}

private object DefaultValue {

  val BUFFER_SIZE = 8192 * 256
  val LINE_FILTER = (s: String) => true
  val TRIPLE_FILTER : RdfTupleFilter[RdfTriple] = (r : RdfTriple) => true
  val QUADRUPLE_FILTER : RdfTupleFilter[RdfQuadruple] = (r : RdfQuadruple) => true
}

trait RdfTupleParser[A <: RdfTriple] {
  def parseTriple(rdfLine: String) : A
}

abstract class RdfTupleLineIterable[A <: RdfTriple](val inputStream : InputStream,
                                                    val bufferSize : Int,
                                                    val lineFilter : LineFilter,
                                                    val tupleFilter : RdfTupleFilter[A]
                                                     ) extends Iterable[A] with RdfTupleParser[A] {

  val reader = new BufferedReader(new InputStreamReader(inputStream), bufferSize)

  def iterator:Iterator[A] =
    Iterator.continually(reader.readLine)
      .takeWhile((s:String) => s != null)
      .withFilter(lineFilter)
      .map(parseTriple(_))
      .withFilter(tupleFilter)
}

trait NTripleParser extends RdfTupleParser[RdfTriple] {

  // this is significantly faster (half an order of magnitude) than split
  def parseTriple(rdfLine: String) : RdfTriple = {
    if (rdfLine.trim().length() == 0) {
      return InvalidRdfTriple()
    }

    val idx = rdfLine.indexOf('\t')
    if (idx <= 0) {
      return InvalidRdfTriple(rdfLine.trim())
    }

    val first = rdfLine.substring(0, idx)
    val idx2 = rdfLine.indexOf('\t', idx + 1)
    if (idx2 <= 0) {
      return InvalidRdfTriple(first,
        rdfLine.substring(idx + 1, rdfLine.length).trim())
    }

    val second = rdfLine.substring(idx + 1, idx2)
    val idx3 = rdfLine.indexOf('\t', idx2 + 1)
    if (idx3 <= 0) {
      return InvalidRdfTriple(first, second, rdfLine.substring(idx2 + 1, rdfLine.length).trim())
    }

    val third = rdfLine.substring(idx2 + 1, idx3)

    val rdfTriple = ValidRdfTriple(first, second, third)

    rdfTriple
  }
}

trait NQuadrupleParser extends RdfTupleParser[RdfQuadruple] {
  def parseTriple(rdfLine: String) : RdfQuadruple = new ValidRdfQuadruple("","","","")
}

class NTripleIterable[A <: RdfTriple](inputStream : InputStream,
                                 bufferSize : Int,
                                 lineFilter : LineFilter,
                                 tupleFilter : RdfTupleFilter[RdfTriple])
    extends RdfTupleLineIterable(inputStream, bufferSize, lineFilter, tupleFilter) with NTripleParser {

    def this(inputStream : InputStream) = this(inputStream, DefaultValue.BUFFER_SIZE, DefaultValue.LINE_FILTER, DefaultValue.TRIPLE_FILTER)
}

object NTripleIterable {

  def apply(inputStream : InputStream) =
    new NTripleIterable(inputStream, DefaultValue.BUFFER_SIZE, DefaultValue.LINE_FILTER, DefaultValue.TRIPLE_FILTER)

  def apply(inputStream : InputStream,
            tripleFilter : RdfTupleFilter[RdfTriple]) =
    new NTripleIterable(inputStream, DefaultValue.BUFFER_SIZE, DefaultValue.LINE_FILTER, tripleFilter)

  def apply(inputStream : InputStream,
            bufferSize : Int,
            lineFilter : LineFilter,
            tripleFilter : RdfTupleFilter[RdfTriple] = DefaultValue.TRIPLE_FILTER) =
    new NTripleIterable(inputStream, bufferSize, lineFilter, tripleFilter)
}

class NQuadrupleIterable[A <: RdfQuadruple](inputStream : InputStream,
                                       bufferSize : Int,
                                       lineFilter : LineFilter,
                                       tupleFilter : RdfTupleFilter[RdfQuadruple])
  extends RdfTupleLineIterable(inputStream, bufferSize, lineFilter, tupleFilter) with NQuadrupleParser {

  def this(inputStream : InputStream) = this(inputStream, DefaultValue.BUFFER_SIZE, DefaultValue.LINE_FILTER, DefaultValue.QUADRUPLE_FILTER)
}
