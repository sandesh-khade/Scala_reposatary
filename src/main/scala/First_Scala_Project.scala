import org.apache.spark.{SparkConf, SparkContext}

object First_Scala_Project
{
  def main(args: Array[String]): Unit =

  {
     var conf = new SparkConf().setMaster("local[*]").setAppName("firstjob")
     var sc =new SparkContext(conf)

     var input = args(0)
     var output =args(1)

    var data =sc.textFile(input)

    var mapdata= data.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    mapdata.saveAsTextFile(output)

  }
}
