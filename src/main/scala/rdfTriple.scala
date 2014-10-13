/*
 * RdfTypes
 * This files contains types to support the processing of Rdf triples.
 *
 * The attempted goal is to create a type hierarchy that enables the processor return an implementation of
 * the RdfTriple Trait (RdfTripleTrait[_,_,_]) which encompasses both valid and invalid responses.
 *
 *
 *
 *
 *
 */

package com.elegantcoding.rdfprocessor.rdftriple

package object types {
  type OptionString = Option[String]
  type RdfTriple = RdfTripleTrait[_,_,_]
  type RdfQuadruple = RdfQuadrupleTrait[_,_,_,_]

  type LineFilter = (String) => Boolean

  type RdfTupleFilter[A <: RdfTriple] = (A) => Boolean
}

import types._

trait RdfTripleTrait[ST,PT,OT] {
  val subject: ST
  val predicate: PT
  val obj: OT

  def isValid: Boolean

  def size: Int

  def subjectString: String

  def predicateString: String

  def objectString: String
}

trait RdfQuadrupleTrait[ST,PT,OT, RT] extends RdfTripleTrait[ST,PT,OT] {
  val provenance: RT
  def provenanceString: String
}

case class ValidRdfTriple(override val subject: String, override val predicate: String, override val obj: String)
  extends RdfTripleTrait[String,String,String] {

  def isValid = true

  def size = 3

  def subjectString = subject

  def predicateString = predicate

  def objectString = obj
}


case class ValidRdfQuadruple(override val subject: String, override val predicate: String, override val obj: String, override val provenance: String)
  extends RdfQuadrupleTrait[String,String,String,String] {

  def isValid = true

  def size = 4

  def subjectString = subject

  def predicateString = predicate

  def objectString = obj

  def provenanceString = provenance
}

abstract class BaseInvalidRdfTriple(override val subject: OptionString, override val predicate: OptionString, override val obj: OptionString)
  extends RdfTripleTrait[OptionString,OptionString,OptionString] {

  def this(rdfTriple : RdfTriple) = this(Some(rdfTriple.subjectString), Some(rdfTriple.predicateString), Some(rdfTriple.objectString))

  //def this(rdfTriple : ValidRdfTriple) = this(Some(rdfTriple.subject), Some(rdfTriple.predicate), Some(rdfTriple.obj))

  private val size_ = (subject, predicate, obj) match {
    case (None, None, None) => 0
    case (None, None, Some(c)) => 1
    case (None, Some(b), Some(c)) => 2
    case _ => 3
  }

  protected def getOptionStringValue(optionString: OptionString): String = {
    optionString match {
      case Some(string) => string
      case _ => ""
    }
  }

  def isValid = false

  def size = size_

  def subjectString = getOptionStringValue(subject)

  def predicateString = getOptionStringValue(predicate)

  def objectString = getOptionStringValue(obj)
}

abstract class BaseInvalidRdfQuadruple(subject: OptionString, predicate: OptionString, obj: OptionString, provenance: OptionString)
  extends BaseInvalidRdfTriple(subject, predicate, obj) with RdfQuadrupleTrait[OptionString,OptionString,OptionString,OptionString]{

  private val size_ = (subject, predicate, obj, provenance) match {
    case (None, None, None, None) => 0
    case (None, None, None, Some(c)) => 1
    case (None, None, Some(b), Some(c)) => 2
    case (None, Some(a), Some(b), Some(c)) => 3
    case _ => 4
  }
}

abstract class InvalidRdfTripleReason(errorString: String)

case class InvalidRdfTripleUnknown(errorString: String) extends InvalidRdfTripleReason(errorString)

case class InvalidRdfTripleSize0() extends InvalidRdfTripleReason("")

case class InvalidRdfTripleSize1() extends InvalidRdfTripleReason("")

case class InvalidRdfTripleSize2() extends InvalidRdfTripleReason("")

case class InvalidRdfTripleFiltered() extends InvalidRdfTripleReason("Filter")

case class InvalidRdfTriple(override val subject: OptionString, override val predicate: OptionString, override val obj: OptionString, invalidRdfTripleReason: InvalidRdfTripleReason)
  extends BaseInvalidRdfTriple(subject, predicate, obj) {
}

case class FilteredInvalidRdfTriple(rdfTriple : RdfTriple, invalidRdfTripleReason: InvalidRdfTripleReason = new InvalidRdfTripleFiltered())
  extends BaseInvalidRdfTriple(rdfTriple) {
}

object InvalidRdfTriple {
  def apply() = new InvalidRdfTriple(None, None, None, new InvalidRdfTripleSize0())

  def apply(subject: String) = new InvalidRdfTriple(Some(subject), None, None, new InvalidRdfTripleSize1())

  def apply(subject: String, predicate: String) = new InvalidRdfTriple(Some(subject), Some(predicate), None, new InvalidRdfTripleSize2())

  def apply(subject: String, predicate: String, obj: String) = new InvalidRdfTriple(Some(subject), Some(predicate), Some(obj), InvalidRdfTripleUnknown(""))
}

