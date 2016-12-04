name := "trucking-topology"

version := "0.1"

organization := "com.hortonworks.orendainx"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(

  "com.hortonworks.orendainx" %% "trucking-shared" % "0.1",

  // HDP will provide it's own Storm Jar, so we mark it as "provided"
  "org.apache.storm" % "storm-core" % "1.0.2" % "provided",
  ("org.apache.storm" % "storm-kafka" % "1.0.2")
      .exclude("org.slf4j", "slf4j-log4j12"),
  ("org.apache.storm" % "storm-hbase" % "1.0.2").
    exclude("org.apache.hbase", "hbase-client"). // temp
    exclude("org.apache.hbase", "hbase-server"). // temp
    exclude("asm", "asm").
    exclude("log4j", "log4j").
    exclude("org.mortbay.jetty", "jsp-2.1"). // vs tomcat:jasper-compiler in itself
    exclude("org.mortbay.jetty", "servlet-api-2.5"). // vs javax.servlet:servlet-api:2.5 in itself and storm-core
    exclude("org.mortbay.jetty", "jsp-api-2.1"), // vs javax.servlet.jsp:jsp-api:2.1 in itself

  // Necessary as it is marked "provided" in storm-kafka and not bundled when assembled
  ("org.apache.kafka" %% "kafka" % "0.10.1.0")
      .exclude("log4j", "log4j")
      .exclude("org.slf4j", "slf4j-log4j12"),

  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

  "org.apache.hbase" % "hbase" % "1.2.4",
  ("org.apache.hbase" % "hbase-common" % "1.2.4"). // THIS
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-beanutils", "commons-beanutils-core"),
  ("org.apache.hbase" % "hbase-client" % "1.2.4")
    .exclude("org.slf4j", "slf4j-log4j12"),

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
