package com.elegantcoding.rdfprocessor.test

import org.scalatest._

import com.elegantcoding.rdfprocessor.rdftriple.InvalidRdfTriple
import com.elegantcoding.rdfprocessor.rdftriple.types.RdfTriple
import com.elegantcoding.rdfprocessor.{NTripleIterable}

class NTripleIterableSpec extends FlatSpec with Matchers {

  "NTripleIterable" should "be able to iterate triples" in {
    val byteArray = "here\tis\ttriple\t.\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleIterable(testStream)
    nts.withFilter(validate(_))

    def validate(triple:RdfTriple):Boolean = {
      if(triple.subjectString != "here") {fail()}
      if(triple.predicateString != "is") {fail()}
      if(triple.objectString != "triple") {fail()}
      true
    }
  }

  it should "be able to iterate invalid triples length 0" in {
    val byteArray = "\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleIterable(testStream)
    nts.toList.head should equal(InvalidRdfTriple())
  }

  it should "be able to iterate invalid triples length 1" in {
    val byteArray = ".\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleIterable(testStream)
    nts.toList.head should equal(InvalidRdfTriple("."))
  }

  it should "be able to iterate invalid triples length 2" in {
    val byteArray = "here\t.\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleIterable(testStream)
    nts.toList.head should equal(InvalidRdfTriple("here", "."))
  }

  it should "be able to iterate invalid triples length 3" in {
    val byteArray = ("1\t2\t3\n").toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleIterable(testStream)
    nts.toList.head should equal(InvalidRdfTriple("1", "2", "3"))
  }

}
