resolvers ++= Seq(
  "socrata releases" at "http://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.1.1")

addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")
