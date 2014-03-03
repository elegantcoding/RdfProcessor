import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "rdfProcessor"


version := "0.1.0"

organization := "com.elegantcoding"

scalaVersion := "2.10.3"

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-feature")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "com.typesafe" % "config" % "1.0.2"
)

seq(lsSettings :_*)

(LsKeys.tags in LsKeys.lsync) := Seq("rdf")

(description in LsKeys.lsync) :=
  "generic RDF processor."
