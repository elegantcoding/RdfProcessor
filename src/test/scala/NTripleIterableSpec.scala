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

  it should "be able to stream and clean" in {
    val byteArray = """<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://www.w3.org/1999/02/22-rdf-syntax-nstype> ↵
<http://xmlns.com/foaf/0.1/Document> .
<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> "N-Triples"@en-US .
<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:art .
<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:dave .
_:art <http://www.w3.org/1999/02/22-rdf-syntax-nstype> <http://xmlns.com/foaf/0.1/Person> .
_:art <http://xmlns.com/foaf/0.1/name> "Art Barstow".
_:dave <http://www.w3.org/1999/02/22-rdf-syntax-nstype> <http://xmlns.com/foaf/0.1/Person> .
_:dave <http://xmlns.com/foaf/0.1/name> "Dave Beckett".""".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleIterable(testStream)
    nts.map {triple =>
      triple
      // TODO clean ns
    }
    //TODO test results
  }

}
