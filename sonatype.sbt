publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

// Random stuff sonatype wants
pomExtra := (
  <url>http://www.github.com/socrata/ssync</url>
  <licenses>
    <license>
      <name>Apache License 2.0</name>
      <url>http://opensource.org/licenses/Apache-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:git@github.com:socrata/ssync.git</connection>
    <url>https://github.com/socrata/ssync</url>
  </scm>
  <developers>
    <developer>
      <name>Robert Macomber</name>
      <email>robert.macomber@socrata.com</email>
      <organization>Socrata</organization>
    </developer>
  </developers>
)

// Random stuff sonatype does not want
pomIncludeRepository := { _ => false }

// These are the defaults, but set them manually just in case

publishMavenStyle := true

publishArtifact in Test := false
