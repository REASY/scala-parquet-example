import com.sksamuel.avro4s.RecordFormat
import models.avro.{ParcelAttribute, Person}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

import scala.collection.GenSeq

object AvroReader extends BenchmarkHelper {
  def main(args: Array[String]): Unit = {
    assert(args.length == 2, "Expected two arguments as a path to parquet files")
    val pathToParcelAttributes = args.head
    val pathToPersons = args.last

    println("***AvroReader***")
    println(s"Path to ParcelAttributes parquet file: $pathToParcelAttributes")
    println(s"Path to Persons parquet file: $pathToPersons")

    bench(maxTries, "Read and transform ParcelAttributes to PrimaryIdToParcelAttribute map", benchmarkParcelAttributes(pathToParcelAttributes))

    bench(maxTries, "Read and transform Persons to HouseholdToPersons map", benchmarkPersons(pathToPersons))
  }

  def benchmarkPersons(path: String): Unit = {
    meter(s"Total time to read and transform Persons to HouseholdToPersons map", {
      val persons = meter("Read persons", readPersons(new Path(path)).toArray.par)
      val size = persons.size
      println(s"Persons size is $size")

      val map = meter("Transform persons to HouseholdToPersons map", asHouseholdToPersons(persons))
      println(s"HouseholdToPersons map size is ${map.size}")
    })
  }

  def benchmarkParcelAttributes(path: String): Unit = {
    meter(s"Total time to read and transform ParcelAttributes to PrimaryIdToParcelAttribute map", {
      val parcelAttributes = meter("Read parcel attributes", readParcelAttributes(new Path(path)).toArray.par)
      val size = parcelAttributes.size
      println(s"ParcelAttributes map size is $size")

      val map = meter("Transform ParcelAttributes to PrimaryIdToParcelAttribute map", asPrimaryIdToParcelAttributesMap(parcelAttributes))
      println(s"PrimaryIdToParcelAttribute map size is ${map.size}")
    })
  }

  def asHouseholdToPersons(seq: GenSeq[Person]): scala.collection.GenMap[Option[Long], GenSeq[Person]] = {
    // We can parallelize here using `.par`
    seq.groupBy(p => p.household_id)
  }

  def asPrimaryIdToParcelAttributesMap(seq: GenSeq[ParcelAttribute]): scala.collection.GenMap[Option[Long], ParcelAttribute] = {
    // We can parallelize here using `.par`
    seq.groupBy(pa => pa.primary_id)
      .map { case (k, v) => k -> v.head }
  }

  def readPersons(path: Path): Iterator[Person] = {
    readAs[Person](path)(RecordFormat[Person])
  }

  def readParcelAttributes(path: Path): Iterator[ParcelAttribute] = {
    readAs[ParcelAttribute](path)(RecordFormat[ParcelAttribute])
  }

  def readAs[T](path: Path)(implicit fmt: RecordFormat[T]): Iterator[T] = {
    val reader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](path).build()
    Iterator.continually(reader.read).takeWhile(_ != null).map(fmt.from)
  }
}
