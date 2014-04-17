autoScalaLibrary := false

autoScalaLibrary in test := true

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
)

scalaVersion := "2.10.4"

