# Parquet vs Csv

# AvroReader
It uses [parquet-mr](https://github.com/apache/parquet-mr) to read parquet file. To decode it to needed type I use [avro4s](https://github.com/sksamuel/avro4s). Avro schemas sits under  [src/main/avro/](src/main/avro/): [ParcelAttribute.avsc](src/main/avro/ParcelAttribute.avsc), [Person.avsc](src/main/avro/Person.avsc). To generate case class from avro schema I use [sbt-avrohugger](https://github.com/julianpeeters/sbt-avrohugger). Link to the data files (Avro and CSV): https://drive.google.com/open?id=1utjIoUDf_qtGZlZivRASOEWdRVdLIwGC
To run the [AvroReader](AvroReader.scala) execute the following:
```sh
 sbt "runMain AvroReader #PATH_TO_PARCEL_ATTR# #PATH_TO_PERSONS#"
```

# CsvReader
To run the [CsvReader](CsvReader.scala) execute the following:
```sh
 sbt "runMain CsvReader c:\\repos\\apache_arrow\\py_arrow\\data\\parcel_attr.csv c:\\repos\\apache_arrow\\py_arrow\\data\\persons.csv"
 ```
## Comparison table
| Reader                              | Type                                                                  | AVG ms |
|-------------------------------------|-----------------------------------------------------------------------|--------|
| ParquetReader(Serial data transf)   | Read and transform ParcelAttributes to PrimaryIdToParcelAttribute map | 5609.3 |
| ParquetReader(Serial data transf)   | Read and transform Persons to HouseholdToPersons map                  | 5445.3 |
| ParquetReader(Parallel data transf) | Read and transform ParcelAttributes to PrimaryIdToParcelAttribute map | 5091.1 |
| ParquetReader(Parallel data transf) | Read and transform Persons to HouseholdToPersons map                  | 5578.0 |
| CsvReader                           | Read and transform ParcelAttributes to PrimaryIdToParcelAttribute map | 6747.8 |
| CsvReader                           | Read and transform Persons to HouseholdToPersons map (typed)          | 6106.8 |

GC logs:
- [AvroReader](https://gceasy.io:443/my-gc-report.jsp?p=c2hhcmVkLzIwMTkvMDEvMTQvLS1BdnJvU2VyaWFsX2djLmxvZy0tMTYtNTQtMjE=&channel=WEB)
- [CsvReader](https://gceasy.io:443/my-gc-report.jsp?p=c2hhcmVkLzIwMTkvMDEvMTQvLS1jc3ZfZ2MubG9nLS0xNi01My00MA==&channel=WEB)

# Convert CSV files to parquet:
I used next Python 3 code to convert CSV files to parquet using [PyArrow](https://arrow.apache.org/docs/python/install.html):
```python
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert(csvPath, parquetPath, compression='NONE'):
	start_time = time.perf_counter()
	csv_stream = pd.read_csv(csvPath)
	csv_stream.info(memory_usage='deep')

	t1 = pa.Table.from_pandas(csv_stream)

	schema = t1.schema
	print(schema)

	writer = pq.ParquetWriter(parquetPath, schema, compression)
	writer.write_table(t1)
	writer.close
	elapsed_time = time.perf_counter() - start_time

	print("Converted '{0}' to '{1}' in {2} seconds".format(csvPath, parquetPath, elapsed_time))

convert('data/persons.csv', 'data/persons.parquet')
convert('data/parcel_attr.csv', 'data/parcel_attr.parquet')
```
