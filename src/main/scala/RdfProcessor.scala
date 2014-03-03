package rdfProcessor

//
// TODO: Add real logging
//
//
//
//
//
//

import java.io.StringWriter
import java.io.PrintWriter

import java.lang.RuntimeException

import collection.JavaConverters._

import annotation.tailrec

//import java.io.Closeable
//import java.lang.Readable

import java.io.{BufferedReader}
//import java.io.{BufferedReader, InputStreamReader, FileInputStream}
//import java.io.{BufferedWriter, OutputStreamWriter, FileOutputStream}
//import java.util.zip.GZIPInputStream

import rdftriple._
import rdftriple.RdfTriple

package object rdfProcessor {

    type RdfTripleFilter = Option[(String, String, String)=>String]

    type RdfLineProcessor = (RdfTriple, String) => Unit

    type CleanerFunction = Option[String => String]

    def checkInSequence[T](item : T, list : Seq[T], compare : (T, T) => Boolean) : Boolean = {

       @tailrec def checkInSequenceR(item : T, list : Seq[T]) : Boolean = {

            if(list.isEmpty) {
                false
            } else if(compare(item, list.head)) {
                true
            } else {
                checkInSequenceR(item, list.tail)
            }
        }

        checkInSequenceR(item, list)
    }

    @inline def stringContainsAllStrings(string : String, stringList : Seq[String]) : Boolean = {

        @inline @tailrec def stringContainsAllStringsR(string : String, stringList : Seq[String]) : Boolean = {

            if(stringList.isEmpty) {
                true
            } else if(!string.contains(stringList.head)) {
                false
            } else {
                stringContainsAllStringsR(string, stringList.tail)
            }
        }

        stringContainsAllStringsR(string, stringList)
    }

    @inline def stringContainsStringConjunctive(string : String, conjunctionList : Seq[Seq[String]]) : Boolean = {

        @inline @tailrec def stringContainsStringConjunctiveR(string:String, conjunctionList : Seq[Seq[String]]) : Boolean = {

            if(conjunctionList.isEmpty) {
                false
            } else if(stringContainsAllStrings(string, conjunctionList.head)) {
                true
            } else {
                stringContainsStringConjunctiveR(string, conjunctionList.tail)
            }
        }

        stringContainsStringConjunctiveR(string, conjunctionList)
    }

    @inline def stringContainsStrings(string : String, stringList : Seq[String]) : Boolean = {

        @inline @tailrec def stringContainsStringsR(string:String, stringList : Seq[String]) : Boolean = {

            if(stringList.isEmpty) {
                false
            } else if(string.contains(stringList.head)) {
                true
            } else {
                stringContainsStringsR(string, stringList.tail)
            }
        }

        stringContainsStringsR(string, stringList)
    }

    def stackTraceToString(throwable : Throwable) {

        val stringWriter = new StringWriter()
        throwable.printStackTrace(new PrintWriter(stringWriter));
        stringWriter.toString();
    }

    def formatTime(elapsedTime : Long) = {

        "%02d:02d:02d".format((elapsedTime / 1000) / 3600,
                              (((elapsedTime / 1000) / 60) % 60),
                              ((elapsedTime / 1000) % 60))
    }
}

import rdfProcessor._


trait RfdCleaner {

    val subjectCleaner : CleanerFunction
    val predicateCleaner : CleanerFunction
    val objectCleaner : CleanerFunction

    def cleanString(string: String, cleaner : CleanerFunction) = cleaner match {
        case Some(function) => function(string)
        case None => string
    }

    def cleanSubject(string: String) = cleanString(string, subjectCleaner)
    def cleanPredicate(string: String) = cleanString(string, predicateCleaner )
    def cleanObject(string: String) = cleanString(string, objectCleaner)
}

object RfdCleaner {

    def apply() = emptyRdfCleaner

    def apply(f1 : String => String, f2 : String => String, f3 : String => String) = new Object with RfdCleaner {
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

    val rdfLineProcessor : RdfLineProcessor
    val processName : String

    val rfdCleaner = emptyRdfCleaner

    def getRdfStream : BufferedReader
    def handleInvalidTriple(rdfTriple : RdfTriple, tripleString : String) = {}

    def printStatus(processName : String, processStartTime : Long, rdfLineCount : Long) = {

        if(rdfLineCount % (ONE_MILLION * 10L) == 0) {

            val curTime = System.currentTimeMillis

            println(processName + ": " + rdfLineCount/1000000 + "M tripleString lines processed" +
                    "; last 10M: " + formatTime(curTime - lastTime) +
                    "; process elapsed: " + formatTime(curTime - processStartTime) +
                    " total elapsed: " + formatTime(curTime - startTime))

            lastTime = curTime
        }
    }

    def validateRdfTriple(subject: String, predicate: String, obj: String) : RdfTriple = {

        ValidRdfTriple(
            rfdCleaner.cleanSubject(subject),
            rfdCleaner.cleanPredicate(predicate),
            rfdCleaner.cleanObject(obj))
    }

    @inline def parseTriple(rdfLine:String) = {

        val idx = rdfLine.indexOf('\t')

        if(idx <= 0)
            InvalidRdfTriple( rdfLine.substring(0, idx) )

        val first = rdfLine.substring(0, idx)

        val idx2 = rdfLine.indexOf('\t', idx + 1)

        if(idx2 <= 0)
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

                    if(rdfTriple.isValid) {

                        rdfLineProcessor(rdfTriple, tripleString)

                    } else {

                        handleInvalidTriple(rdfTriple, tripleString)
                    }
                })
        }
        finally
        {
            rdfStream.close
        }
    }
}


abstract class RdfProcessor extends App
{
    init

    try {
        processFiles(getRdfFileProcessorSeq)
    }
    finally
    {
        shutdown
    }

    def getRdfFileProcessorSeq : Seq[RdfFileProcessor]

    def init : Unit = {}
    def shutdown : Unit = {}


    def processFiles(rdfFileProcessorSeq : Seq[RdfFileProcessor]) = {

        for(rdfFileProcessor <- rdfFileProcessorSeq) {

            try {

                rdfFileProcessor.processRdfFile

            }
            //catch {
            //
            //    case throwable : Throwable => {
            //
            //        println(stackTraceToString(throwable))
            //    }
            //}
            finally {
            }
        }
    }
}
