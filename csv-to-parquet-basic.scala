// scalastyle:off println

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Csv2Parquet {
      def main(args: Array[String]): Unit = {
            // recive file.csv from argparser
            val filein = args[0]
            // recive the folder name to be created for save the parquet data
            val fileout = args[1]

            val conf = new SparkConf().setMaster("local[*]").setAppName("csv2parquet")
            val sc = new SparkContext(conf)

            def convert(filename: String, outparquet: String): Unit = {
                  // read csv defining header
                  val df = sc.read.option("header","true").csv(filename)
                  // save to parquet
                  df.write.parquet(outparquet)
              }

            convert(filein, fileout)
      }
}
      
// scalastyle:on println
