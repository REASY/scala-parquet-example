import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util

import models.avro.Person
import org.supercsv.io.CsvMapReader
import org.supercsv.prefs.CsvPreference

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CsvReader extends BenchmarkHelper{
  import scala.language.reflectiveCalls

  def main(args: Array[String]): Unit = {
    assert(args.length == 2, "Expected two arguments as a path to parquet files")
    val pathToParcelAttributes = args.head
    val pathToPersons = args.last

    println("***CsvReader***")
    println(s"Path to ParcelAttributes csv file: $pathToParcelAttributes")
    println(s"Path to Persons csv file: $pathToPersons")

    bench(maxTries, "Read and transform ParcelAttributes to PrimaryIdToParcelAttribute map (untyped)", benchmarkParcelAttributes(pathToParcelAttributes))

    bench(maxTries, "Read and transform Persons to HouseholdToPersons map (typed)", benchmarkPersons(pathToPersons))
  }

  def benchmarkPersons(path: String): Unit = {
    val householdToPersons: collection.Map[Long, ListBuffer[Person]] = meter(
      "Total time to read and transform Persons to HouseholdToPersons map (typed)",
      readPersonsFile(path))
    println(s"HouseholdToPersons map size is ${householdToPersons.size}")
  }

  def benchmarkParcelAttributes(path: String): Unit = {
    val parcelAttrs: Map[String, util.Map[String, String]] = meter(
      "Total time to read and transform ParcelAttributes to PrimaryIdToParcelAttribute map (untyped)",
      readParcelAttrFile(path))
    println(s"PrimaryIdToParcelAttribute map size is ${parcelAttrs.size}")
  }

  def readParcelAttrFile(filePath: String): Map[String, java.util.Map[String, String]] = {
    val map = readCsvFileByLine(filePath, mutable.HashMap[String, java.util.Map[String, String]]()) {
      case (line, acc) =>
        val _line = new java.util.TreeMap[String, String]()
        _line.put("x", line.get("x"))
        _line.put("y", line.get("y"))
        acc += ((line.get("primary_id"), _line))
    }
    map.toMap
  }

  def readPersonsFile(filePath: String): scala.collection.Map[Long, ListBuffer[Person]] = {
    val map = readCsvFileByLine(filePath, mutable.HashMap[Long, ListBuffer[Person]]()) {
      case (line, acc) =>
        val personId = line.get("person_id").toLong
        val householdId = line.get("household_id").toLong
        val age = line.get("age").toLong
        val person = Person(Some(personId),Some(age), Some(householdId))

        acc.get(householdId) match {
          case Some(buffer) =>
            buffer += person
          case None =>
            acc += ((householdId, ListBuffer(person)))
        }
        acc
    }
    map
  }

  def readCsvFileByLine[A](filePath: String, z: A)(readLine: (java.util.Map[String, String], A) => A): A = {
    val infile = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))
    using(new CsvMapReader(infile, CsvPreference.STANDARD_PREFERENCE)) {
      mapReader =>
        var res: A = z
        val header = mapReader.getHeader(true)
        var line: java.util.Map[String, String] = mapReader.read(header: _*)
        while (null != line) {
          res = readLine(line, res)
          line = mapReader.read(header: _*)
        }
        res
    }
  }

  def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }
}
