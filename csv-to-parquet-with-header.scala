def main(args: Array[String]): Unit = {
      //recebe arquivo.csv do argparser
      val filein = args[0]
      //recebe nome do folder a ser criado para salvar o parquet do argparser
      val fileout = args[1] 
      
      def convert(filename: String, outparquet: String): Unit = {
            //le csv definindo o cabecalho
            val df = spark.read.option("header","true").csv(filename)
            //salva para parquet
            df.write.parquet(outparquet)
        }

      convert(filein, fileout)
}
