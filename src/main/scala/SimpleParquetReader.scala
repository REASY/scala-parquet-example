import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}

object SimpleParquetReader {
  def main(args: Array[String]): Unit = {
    assert(args.nonEmpty, "Expected path to the file, but got nothig")

    val parquetPath = new Path(args.head)
    println(s"Path to parquet file: ${parquetPath}")

    showMetadata(parquetPath)

    // Read some rows
    read(parquetPath).take(20).zipWithIndex.foreach { case (row, idx)=>
      println(s"$idx: $row")
    }
  }

  def showMetadata(parquetPath: Path): Unit = {
    val cfg = new Configuration
    // Create parquet reader
    val rdr = ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, cfg))
    try {
      println(s"Rows: ${rdr.getRecordCount}")
      // Get parquet schema
      val schema = rdr.getFooter.getFileMetaData.getSchema
      println("Parquet schema: ")
      println("#############################################################")
      print(rdr.getFooter.getFileMetaData.getSchema)
      println("#############################################################")
      println

      println("Metadata: ")
      println("#############################################################")
      println(rdr.getFooter.getFileMetaData.getKeyValueMetaData)
      println("#############################################################")
      println
    }
    finally {
      rdr.close()
    }
  }

  def read(path: Path): Iterator[GenericRecord] = {
    val reader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](path).build()
    Iterator.continually(reader.read).takeWhile(_ != null)
  }
}
