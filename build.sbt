import com.socrata.cloudbeessbt.SocrataCloudbeesSbt
import com.typesafe.tools.mima.plugin.MimaKeys._

seq(SocrataCloudbeesSbt.socrataBuildSettings : _*)

seq(SocrataCloudbeesSbt.socrataProjectSettings(assembly=false) : _*)

name := "ssync"

javacOptions in doc := Seq()

previousArtifact <<= (name) { name => Some("com.socrata" % name % "1.0.0") }

packageOptions in (Compile, packageBin) <++= (mainClass in Compile) map {
  case Some(mc) =>
    Seq(Package.ManifestAttributes(java.util.jar.Attributes.Name.MAIN_CLASS -> mc))
  case None =>
    Nil
}

autoScalaLibrary := false

autoScalaLibrary in test := true

crossPaths := false

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
)

scalaVersion := "2.10.4"
