import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 01-Sep-16.
  */
object SparkActions {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkActions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val nums = sc.parallelize(Array(5,6,7,8,9,3,2,1))
    // Retrieve RDD contents as a local collection
    nums.collect() // => [5,6,7,8,9,3,2,1]
    //Return first K elements
    nums.take(2) // => [5,6]
    //Count number of elements
    nums.count() // => 8
    //Merge elements with an associative function
    nums.reduce((x, y) => (x + y)) // => 41
    //Write elements to a text file
    nums.saveAsTextFile("Actions_Out.txt")

  }
}
