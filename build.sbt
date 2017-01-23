name := "trucking-topology"

version := "0.3.1"

organization := "com.hortonworks.orendainx"

scalaVersion := "2.11.8"

// Needed until registry rebuilt w/o Scala version limitation
resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(

  "com.hortonworks.orendainx" %% "trucking-shared" % "0.3.1",

  "org.apache.nifi" % "nifi-storm-spout" % "1.1.0",

  ("com.hortonworks.registries" % "schema-registry-serdes" % "0.1.0-SNAPSHOT")
      .exclude("org.slf4j", "log4j-over-slf4j")
      //.exclude("commons-collections", "commons-collections")
      //.exclude("commons-beanutils", "commons-beanutils")
      .exclude("commons-beanutils", "commons-beanutils-core"),
      //.exclude("org.glassfish.hk2.external", "aopalliance-repackaged")
      //.exclude("org.glassfish.hk2.external", "javax.inject"),
      //.exclude("com.sun.jersey", "jersey-core"),
      //.exclude("javax.ws.rs", "javax.ws.rs-api"),


  // HDP will provide it's own Storm Jar, so we mark it as "provided"
  "org.apache.storm" % "storm-core" % "1.0.2" % "provided",




  /*("org.apache.hadoop" % "hadoop-common" % "2.7.3")
    .exclude("commons-collections", "commons-collections")
    .exclude("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("org.slf4j", "slf4j-log4j12"),*/
  //("org.apache.hadoop" % "hadoop-client" % "2.7.3")
  //  .exclude("commons-collections", "commons-collections")
  //  .exclude("commons-beanutils", "commons-beanutils") // vs below and commons-collections:commons-collections in itself
  //  .exclude("commons-beanutils", "commons-beanutils-core") // vs above and commons-collections:commons-collections in itself
  //  .exclude("org.slf4j", "slf4j-log4j12"),
  // TODO: Review: Verify above is not included with Storm, and does not just require classpath fix


  "com.typesafe" % "config" % "1.3.1",
  "com.github.pathikrit" %% "better-files" % "2.16.0",

  // Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "commons", "collections", xs @ _*)      => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// Export Jars, not class files
exportJars := true

scalacOptions ++= Seq("-feature")
