import java.io.{StringReader, BufferedReader}
import org.scalatest._
import com.elegantcoding.rdfProcessor._
import rdftriple.rdftriple.RdfTriple
import rdftriple.ValidRdfTriple

class RdfProcessorSpec extends FlatSpec with Matchers {

  class ConcreteRdfFileProcessor extends RdfFileProcessor {
    override val getRdfStream = new BufferedReader(new StringReader("..."))
    override val processName = "test"
    override val rdfLineProcessor = (triple:RdfTriple, line:String) => {}
  }

  "RdfFileProcessor.parseTriple" should "be able to parse a triple" in {
    val res = (new ConcreteRdfFileProcessor).parseTriple("hello\tthis\ttest\t.")
    res should equal(ValidRdfTriple("hello","this","test"))
  }

}
