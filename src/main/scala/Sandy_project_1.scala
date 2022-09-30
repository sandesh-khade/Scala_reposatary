import org.apache.spark.{SparkConf, SparkContext}

object Sandy_project_1
{
  def main(args: Array[String]): Unit =
  {
    var conf = new SparkConf().setMaster("local[*]").setAppName("secondjob")
    var sc = new SparkContext(conf)

    var input = args(0)
    var output = args(1)

    var data =sc.textFile(input)
    var result = data.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)

    result.repartition(4).saveAsTextFile(output)

  }
}
