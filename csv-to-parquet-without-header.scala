// scalastyle:off println

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Csv2Parquet {
      def main(args: Array[String]): Unit = {
            val filein = args[0]
            //recebe nome do folder a ser criado para salvar o parquet do argparser
            val fileout = args[1]
            // cria o sc e referencia para o sqlc
            val conf = new SparkConf().setMaster("local[*]").setAppName("csv2parquet")
            val sc = new SparkContext(conf)
            val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        
            def convert(filename: String, outparquet: String, schema: StructType): Unit = {
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
                              // editar conforme a estrutura dos dados
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
