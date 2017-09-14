// scalastyle:off println

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Csv2Parquet {
      def main(args: Array[String]): Unit = {
            //recebe arquivo.csv do argparser
            val filein = args[0]
            //recebe nome do folder a ser criado para salvar o parquet do argparser
            val fileout = args[1]

            val conf = new SparkConf().setMaster("local[*]").setAppName("csv2parquet")
            val sc = new SparkContext(conf)

            def convert(filename: String, outparquet: String): Unit = {
                  //le csv definindo o cabecalho
                  val df = sc.read.option("header","true").csv(filename)
                  //salva para parquet
                  df.write.parquet(outparquet)
              }

            convert(filein, fileout)
      }
}
      
// scalastyle:on println
