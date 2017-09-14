def convert(filename: String, schema: StructType, outparquet: String) {
  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(schema)
    .option("delimiter","|")
    .option("nullValue","")
    .option("treatEmptyValuesAsNulls","true")
    .load(filename)
  df.write.parquet(outparquet)
}

schema= StructType(Array(
            StructField("index", IntegerType(), True),
            StructField("arrival_time", DoubleType(), True),
            StructField("creation_time", DoubleType(), True),
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True),
            StructField("z", DoubleType(), True),
            StructField("user", StringType(), True),
            StructField("model", StringType(), True),
            StructField("device", StringType(), True),
            StructField("gt", StringType(), True)
        ))

convert("massive.csv", schema,  "massiveparquet")
