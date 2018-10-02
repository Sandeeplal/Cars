import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object CarsDB {

  def main(args: Array[String]): Unit = {
    val spark = new sql.SparkSession.Builder().master("local[*]").appName("Cars").getOrCreate()

    val data = spark.read.option("delimiter","\t").option("header","true").csv("C:/Users/sandeep/IdeaProjects/Cars/cars.txt")

    data.select("make","Model").show()

    data.select("make","Model").groupBy("make").count.show()

    data.select("make","Model").orderBy("make").show()

    data.filter("Origin=='American'").show()

    data.createOrReplaceTempView("Cars")

    spark.sql("Select * from Cars where Origin='American'").show()

    spark.sql("Select distinct avg(Weight) over(partition by Origin),Origin from Cars").show()

    spark.sql("Select distinct sum(Weight) over(partition by Origin) as SUMWeight ,Origin from Cars").show()



  }

}
