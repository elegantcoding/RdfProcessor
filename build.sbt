name := "rdf-processor"

version := "0.4.0"

organization := "com.elegantcoding"

scalaVersion := "2.11.1"

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-feature")

publishTo := Some(Resolver.file("file",  new File( "c:/usr/elegantcoding.github.io/repo/releases" )) )

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "commons-io" % "commons-io" % "2.4"
)

seq(lsSettings :_*)

(LsKeys.tags in LsKeys.lsync) := Seq("rdf", "ntriples", "nquads")

(description in LsKeys.lsync) :=
  "generic RDF processor."

(externalResolvers in LsKeys.lsync) := Seq(
  "elegantcoding releases" at "http://elegantcoding.github.io/repo/releases")

instrumentSettings

coverallsSettings

CoverallsKeys.coverallsToken := Some("KmSfzLvg19I8FOs6PX94bbdZbwcR0ly01")
