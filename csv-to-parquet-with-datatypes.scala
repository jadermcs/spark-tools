// scalastyle:off println

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

object Csv2Parquet {
      def main(args: Array[String]): Unit = {
            val filein = args[0]
            val fileout = args[1]
            // create a spark context with desired configs and pass to the sqlcontext
            val conf = new SparkConf().setMaster("local[*]").setAppName("csv2parquet")
            val sc = new SparkContext(conf)
            val sqlContext = new SQLContext(sc)
        
            def convert(filename: String, outparquet: String, schema: StructType): Unit = {
                  val df = sqlContext.read
                            .format("com.databricks.spark.csv")
                            //enables read fist line as header, if schema passed, disable the 2 subsequent lines
                            .option("header", "true")
                            .option("inferSchema", "true") // infer the datatype for the columns
                            //.schema(schema)
                            .option("nullValue","NA")
                            .option("treatEmptyValuesAsNulls","true")
                            .load(filename)
                  
                  df.write.parquet(outparquet)
              }
            schema = StructType(Array(
                              // define the datatypes for each column
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

            convert(filein, fileout, schema)
      }
}
      
// scalastyle:on println
