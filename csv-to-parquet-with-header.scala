def convert(filename: String, outparquet: String) {
      val df = spark.read.option("header","true").csv(filename)
      df.write.parquet(outparquet)
  }

convert("massive.csv", "massiveparquet")
