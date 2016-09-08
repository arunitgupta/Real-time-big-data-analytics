import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 01-Sep-16.
  */
object SparkMultipleDatasets {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkActions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val visits = sc.parallelize(Array(("facebook.com", "1.2.3.4"),("google.com", "3.4.5.6"),("facebook.com", "1.3.3.1")))
    val pageNames = sc.parallelize(Array(("facebook.com", "Home"), ("google.com", "About")))
    visits.join(pageNames)
    // ("facebook.com", ("1.2.3.4", "Home"))
    // ("facebook.com", ("1.3.3.1", "Home"))
    // ("google.com", ("3.4.5.6", "About"))
    visits.cogroup(pageNames)
    // ("facebook.com", (Seq("1.2.3.4", "1.3.3.1"), Seq("Home")))
    // ("google.com", (Seq("3.4.5.6"), Seq("About")))
visits.saveAsTextFile("out.txt");
  }
}
