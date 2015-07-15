name := "ssync"

javacOptions in doc := Seq()

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
  "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
)
