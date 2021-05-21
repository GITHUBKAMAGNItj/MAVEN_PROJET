import org.apache.spark.sql._
import org.apache.spark._

object sparka {

  def sessions_spark(): Unit = {

    System.setProperty("winutils", "C:\\HADOOP\\bin")
    val ss = SparkSession.builder()
      .master("local[*]")
      .appName("ma première aplication")
      .getOrCreate()

    val rdd1 = ss.sparkContext.parallelize(Seq("ma première application Spark. je suis un vrai DIGBA"))
    rdd1.foreach(l => println(l))


    val df1 = ss.read
      .format(source = "csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path = "D:\\DOSSIER DATA\\HADOOP_ORDRE\\orders_csv.csv")

    df1.show(numRows = 10) //Voir les 10 premieres lignes du csv
    //df1.printSchema()

    // SQL sous spark sans hive
    //df1.createOrReplaceTempView(viewName = "orders") // juste pour ma session
    //df_1.createGlobalTempView()   //rend le metastore de spark disponible sur plusieurs sessions
    // pour lancer le sql, on fait

    //ss.sql(sqlText = "SELECT * FROM orders WHERE city = 'NEWTON'").explain()
    // .explain() permet de voir comment l'optimisateur de spark execute la requete SQL
  }

def main(args: Array[String]): Unit = {
  sessions_spark()
}


}



