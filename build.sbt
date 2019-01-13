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

val jfrWithMemAndExceptions = Seq("-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder", "-XX:+UnlockDiagnosticVMOptions",
  "-XX:+DebugNonSafepoints", "-XX:StartFlightRecording=delay=0s,duration=60m,name=mem_ex,filename=recording.jfr,settings=profile_heap_exception",
  "-XX:FlightRecorderOptions=disk=true,maxage=10h,dumponexit=true,loglevel=info")

fork in run := true

javaOptions in run ++= heapOptions // ++ jfrWithMemAndExceptions

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue