import java.io.{StringReader, BufferedReader}
import org.scalatest._
import com.elegantcoding.rdfProcessor._
import rdftriple.rdftriple.RdfTriple
import rdftriple.{InvalidRdfTripleSize0, InvalidRdfTriple, ValidRdfTriple}

class NTripleStreamSpec extends FlatSpec with Matchers {

  "NTripleStream" should "be able to stream triples" in {
    val byteArray = "here\tis\ttriple\t.\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleStream(testStream)
    nts.stream.withFilter(validate(_))

    def validate(rdfTriple:RdfTriple):Boolean = {
      if(rdfTriple.subjectString != "here") {fail()}
      if(rdfTriple.predicateString != "is") {fail()}
      if(rdfTriple.objString != "triple") {fail()}
      true
    }
  }

  it should "be able to stream invalid triples length 0" in {
    val byteArray = "\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleStream(testStream)
    nts.stream.toList.head should equal(InvalidRdfTriple())
  }

  it should "be able to stream invalid triples length 1" in {
    val byteArray = ".\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleStream(testStream)
    nts.stream.toList.head should equal(InvalidRdfTriple("."))
  }

  it should "be able to stream invalid triples length 2" in {
    val byteArray = "here\t.\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleStream(testStream)
    nts.stream.toList.head should equal(InvalidRdfTriple("here", "."))
  }

}
