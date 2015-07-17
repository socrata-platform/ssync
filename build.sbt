name := "ssync"

javacOptions in doc := Seq()

// SBT 0.13 doesn't auto-detect Java mains anymore?
mainClass in (Compile) := Some("com.socrata.ssync.SSync")

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
