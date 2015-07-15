resolvers ++= Seq(
  "socrata releases" at "http://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.5.2")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
