name := "trucking-topology"

version := "0.3"

organization := "com.hortonworks.orendainx"

scalaVersion := "2.11.8"

// Needed until registry rebuilt w/o Scala version limitation
resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(

  "com.hortonworks.orendainx" %% "trucking-shared" % "0.3",

  "org.apache.nifi" % "nifi-storm-spout" % "1.1.0",

  "com.hortonworks.registries" % "schema-registry-serdes" % "0.1.0-SNAPSHOT",

  // HDP will provide it's own Storm Jar, so we mark it as "provided"
  "org.apache.storm" % "storm-core" % "1.0.2" % "provided",

  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

  ("org.apache.hadoop" % "hadoop-common" % "2.7.3")
    .exclude("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("org.slf4j", "slf4j-log4j12"),
  ("org.apache.hadoop" % "hadoop-client" % "2.7.3").
    exclude("commons-beanutils", "commons-beanutils"). // vs below and commons-collections:commons-collections in itself
    exclude("commons-beanutils", "commons-beanutils-core") // vs above and commons-collections:commons-collections in itself
    .exclude("org.slf4j", "slf4j-log4j12"),
  // TODO: Review: Verify above is not included with Storm, and does not just require classpath fix

  "com.typesafe" % "config" % "1.3.1",
  "com.github.pathikrit" %% "better-files" % "2.16.0"
)

// Export Jars, not class files
exportJars := true

scalacOptions ++= Seq("-feature")
