package com.elegantcoding.rdfProcessor

import rdftriple.rdftriple.RdfTriple
import java.io.{StringWriter, PrintWriter, BufferedReader}
import rdftriple._
import grizzled.slf4j.Logger

package object rdfProcessor {
  val logger = Logger("com.elegantcoding.rdf-processor")

  type RdfTripleFilter = Option[(String, String, String) => String]

  type RdfLineProcessor = (RdfTriple, String) => Unit

  type CleanerFunction = Option[String => String]

  def stackTraceToString(throwable: Throwable) {
    val stringWriter = new StringWriter()
    throwable.printStackTrace(new PrintWriter(stringWriter))
    stringWriter.toString()
  }

  def formatTime(elapsedTime: Long) = {
    "%02d:%02d:%02d".format((elapsedTime / 1000) / 3600,
      (((elapsedTime / 1000) / 60) % 60),
      ((elapsedTime / 1000) % 60))
  }
}

import rdfProcessor._

trait RfdCleaner {

  val subjectCleaner: CleanerFunction
  val predicateCleaner: CleanerFunction
  val objectCleaner: CleanerFunction

  def cleanString(string: String, cleaner: CleanerFunction) = cleaner match {
    case Some(function) => function(string)
    case None => string
  }

  def cleanSubject(string: String) = cleanString(string, subjectCleaner)

  def cleanPredicate(string: String) = cleanString(string, predicateCleaner)

  def cleanObject(string: String) = cleanString(string, objectCleaner)
}

object RfdCleaner {

  def apply() = emptyRdfCleaner

  def apply(f1: String => String, f2: String => String, f3: String => String) = new Object with RfdCleaner {
    override val subjectCleaner = Some(f1)
    override val predicateCleaner = Some(f2)
    override val objectCleaner = Some(f3)
  }
}

object emptyRdfCleaner extends RfdCleaner {
  override val subjectCleaner = None
  override val predicateCleaner = None
  override val objectCleaner = None
}


abstract class RdfFileProcessor {

  val ONE_MILLION = 1000000L

  val startTime = System.currentTimeMillis
  var lastTime = System.currentTimeMillis

  val rdfLineProcessor: RdfLineProcessor
  val processName: String

  val rfdCleaner = emptyRdfCleaner

  def getRdfStream: BufferedReader

  def handleInvalidTriple(rdfTriple: RdfTriple, tripleString: String) = {}

  def printStatus(processName: String, processStartTime: Long, rdfLineCount: Long) = {

    if (rdfLineCount % (ONE_MILLION * 10L) == 0) {
      val curTime = System.currentTimeMillis
      println(processName + ": " + rdfLineCount / 1000000 + "M tripleString lines processed" +
        "; last 10M: " + formatTime(curTime - lastTime) +
        "; process elapsed: " + formatTime(curTime - processStartTime) +
        " total elapsed: " + formatTime(curTime - startTime))
      lastTime = curTime
    }
  }

  def validateRdfTriple(subject: String, predicate: String, obj: String): RdfTriple = {
    ValidRdfTriple(
      rfdCleaner.cleanSubject(subject),
      rfdCleaner.cleanPredicate(predicate),
      rfdCleaner.cleanObject(obj))
  }

  @inline def parseTriple(rdfLine: String) = {

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

    validateRdfTriple(first, second, third)
  }

  @inline def processRdfFile() = {

    val processStartTime = System.currentTimeMillis

    val rdfStream = getRdfStream

    var rdfLineCount = 0

    try {
      Stream.continually(rdfStream.readLine)
        .takeWhile(_ != null)
        .foreach((tripleString) => {

        rdfLineCount += 1

        printStatus(processName, processStartTime, rdfLineCount)

        val rdfTriple = parseTriple(tripleString)

        if (rdfTriple.isValid) {

          rdfLineProcessor(rdfTriple, tripleString)

        } else {

          handleInvalidTriple(rdfTriple, tripleString)
        }
      })
    }
    finally {
      rdfStream.close
    }
  }
}


abstract class RdfProcessor extends App {
  init

  try {
    processFiles(getRdfFileProcessorSeq)
  }
  finally {
    shutdown
  }

  def getRdfFileProcessorSeq: Seq[RdfFileProcessor]

  def init: Unit = {}

  def shutdown: Unit = {}


  def processFiles(rdfFileProcessorSeq: Seq[RdfFileProcessor]) = {

    for (rdfFileProcessor <- rdfFileProcessorSeq) {
      try {
        rdfFileProcessor.processRdfFile
      }
      finally {
      }
    }
  }
}
