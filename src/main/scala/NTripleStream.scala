package com.elegantcoding.rdfProcessor

import java.io._
import rdftriple.InvalidRdfTriple

class NTripleInputStream(is:InputStream) {
  val reader = new BufferedReader(new InputStreamReader(is))

  def stream:Stream[(String,String,String)] =
    Stream.continually(reader.readLine)
      .takeWhile((s:String) => s != null)
      .map(parseTripleSplit(_))

  def parseTripleSplit(triple:String):(String,String,String) = {
    val Array(subj:String, pred:String, obj:String, _:String) = triple.split("\t")
    (subj, pred, obj)
  }

  def parseTriple(rdfLine: String) = {
    val idx = rdfLine.indexOf('\t')
    if (idx <= 0)
      InvalidRdfTriple(rdfLine.substring(0, idx))
    val first = rdfLine.substring(0, idx)
    val idx2 = rdfLine.indexOf('\t', idx + 1)
    if (idx2 <= 0)
      InvalidRdfTriple(first,
        rdfLine.substring(idx + 1, idx2))
    val second = rdfLine.substring(idx + 1, idx2)
    val idx3 = rdfLine.lastIndexOf('\t')
    val third = rdfLine.substring(idx2 + 1, idx3)
    (first, second, third)
  }
}