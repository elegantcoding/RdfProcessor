import java.io.{StringReader, BufferedReader}
import org.scalatest._
import com.elegantcoding.rdfProcessor._
import rdftriple.rdftriple.RdfTriple
import rdftriple.ValidRdfTriple

class NTripleInputStreamSpec extends FlatSpec with Matchers {

  "NTripleInputStream" should "be able to stream triples" in {
    val byteArray = "here\tis\ttriple\t.\n".toCharArray.map(_.toByte)
    val testStream = new java.io.ByteArrayInputStream(byteArray)

    val nts = new NTripleStream(testStream)
    nts.stream.map(rdf => validate(rdf))

    def validate(rdfTriple:RdfTriple):RdfTriple = {
      if(rdfTriple.subjectString != "here") {fail()}
      if(rdfTriple.predicateString != "is") {fail()}
      if(rdfTriple.objString != "triple") {fail()}
      rdfTriple
    }
  }

}
