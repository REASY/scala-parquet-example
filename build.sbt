import sbt.Keys._

version := "0.0.1"
scalaVersion := "2.12.8"
name := "parquet-reader"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-avro" % "1.10.0",
  "org.apache.parquet" % "parquet-hadoop" % "1.10.0",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "net.sf.supercsv" % "super-csv" % "2.4.0",
  "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.3"
)
// initial Java heap size: 1G, maximum Java heap size: 4G
val heapOptions = Seq("-Xms1G", "-Xmx4G")

// On the running machine there should be file /usr/lib/jvm/java-8-oracle/jre/lib/jfr/profile_heap_exception.jfc  with content from
// https://pastebin.com/N3uuUfPz - it's Java Mission Control with metrics about heap allocation and details about exceptions
val jfrWithMemAndExceptions = Seq("-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder", "-XX:+UnlockDiagnosticVMOptions",
  "-XX:+DebugNonSafepoints", "-XX:StartFlightRecording=delay=0s,duration=60m,name=mem_ex,filename=recording.jfr,settings=profile_heap_exception",
  "-XX:FlightRecorderOptions=disk=true,maxage=10h,dumponexit=true,loglevel=info")

val logGC = Seq("-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-Xloggc:gc.log")

fork in run := true

javaOptions in run ++= heapOptions ++ logGC // ++ jfrWithMemAndExceptions

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue