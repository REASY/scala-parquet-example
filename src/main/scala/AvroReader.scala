import com.sksamuel.avro4s.RecordFormat
import models.avro.{ParcelAttribute, Person}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

import scala.collection.GenSeq

object AvroReader extends BenchmarkHelper {
  def main(args: Array[String]): Unit = {
    println("***AvroReader***")

    bench(maxTries, "Read and transform ParcelAttributes to PrimaryIdToParcelAttribute map", benchmarkParcelAttributes)

    bench(maxTries, "Read and transform Persons to HouseholdToPersons map", benchmarkPersons)
  }

  def benchmarkPersons: Unit = {
    val path = new Path("c:\\repos\\apache_arrow\\py_arrow\\data\\persons.parquet")

    meter(s"Total time to read and transform Persons to HouseholdToPersons map", {
      val persons = meter("Read persons", readPersons(path).toArray.par)
      val size = persons.size
      println(s"Persons size is $size")

      val map = meter("Transform persons to HouseholdToPersons map", asHouseholdToPersons(persons))
      println(s"HouseholdToPersons map size is ${map.size}")
    })
  }

  def benchmarkParcelAttributes: Unit = {
    val path = new Path("c:\\repos\\apache_arrow\\py_arrow\\data\\parcel_attr.parquet")
    meter(s"Total time to read and transform ParcelAttributes to PrimaryIdToParcelAttribute map", {
      val parcelAttributes = meter("Read parcel attributes", readParcelAttributes(path).toArray.par)
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
