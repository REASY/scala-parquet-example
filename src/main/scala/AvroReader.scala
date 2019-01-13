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

    bench(maxTries, "benchmarkParcelAttributes", benchmarkParcelAttributes)

    bench(maxTries, "benchmarkPersons", benchmarkPersons)
  }

  def benchmarkPersons: Unit = {
    val path = new Path("c:\\repos\\apache_arrow\\py_arrow\\data\\persons.parquet")

    meter(s"Total time to process Person from '${path.toString}'", {
      val persons = meter("readPersons", readPersons(path).toArray.par)
      val size = persons.size
      println(s"readPersonsFile: persons size is $size")

      val map = meter("asHouseholdToPersons", asHouseholdToPersons(persons))
      println(s"asHouseholdToPersons:  map size is ${map.size}")
    })
  }

  def benchmarkParcelAttributes: Unit = {
    val path = new Path("c:\\repos\\apache_arrow\\py_arrow\\data\\parcel_attr.parquet")
    meter(s"Total time to process ParcelAttribute from '${path.toString}'", {
      val parcelAttributes = meter("readParcelAttributes", readParcelAttributes(path).toArray.par)
      val size = parcelAttributes.size
      println(s"readParcelAttributes: parcelAttributes size is $size")

      val map = meter("asPrimaryIdToParcelAttributesMap", asPrimaryIdToParcelAttributesMap(parcelAttributes))
      println(s"asPrimaryIdToParcelAttributesMap: map size is ${map.size}")
    })
  }

  def asHouseholdToPersons(seq: GenSeq[Person]): scala.collection.GenMap[Option[Long], GenSeq[Person]] = {
    seq.groupBy(p => p.household_id)
  }

  def asPrimaryIdToParcelAttributesMap(seq: GenSeq[ParcelAttribute]): scala.collection.GenMap[Option[Long], ParcelAttribute] = {
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
