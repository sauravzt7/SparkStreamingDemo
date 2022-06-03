package org.apache.spark.examples.streaming
import org.apache.spark.sql.SparkSession
import com.google.common.collect.Table
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.
import org.apache.spark.sql.{Row, SQLContext, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, lit, struct, when}
import org.apache.spark.{SparkConf, SparkContext}

object SparkExamples {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL examples")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    var totalRecords : Long = 0;
    var totalPushEvents : Long = 0;

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //read json file in dataframes

    val df = sqlContext.read.json("/Users/s0k08m1/Downloads/data.json")
//    val ds = spark.read.json("/Users/s0k08m1/Downloads/data.json")

    totalRecords = df.count()

    val dfPushEvents = df.filter( $"type" === "PushEvent")
    val dfPullEvents = df.filter($"type" === "PullRequestEvent")

    totalPushEvents = dfPushEvents.count()
    val dfActor = df.select("actor")

//    println(ds.head())

//    val copyDf = df.withColumn("actor", struct(col("actor.*"), lit("Hello").as("activityLevel")))

    val df2 = dfPushEvents.groupBy(dfPushEvents("actor.id").as("copyID"), dfPushEvents("type").as("copyType")).count().orderBy(col("count").desc)
          .withColumn("activityLevel",
            when(col("count") >= 0 &&  col("count") <= 2, "InActive")
              .when(col("count") > 2 && col("count") <= 5, "ModerateActive")
              .otherwise("MostActive"))

       val topTenUser = dfPushEvents.groupBy(dfPushEvents("actor.id"), dfPushEvents("type")).count().orderBy(col("count").desc)
       val topTenRepo = dfPullEvents.groupBy(dfPullEvents("repo.name"), dfPullEvents("type")).count().orderBy(col("count").desc)

    // for each user have a parameter which gives type and how many type of events he has done


    println(totalRecords)
    println(totalPushEvents)
    dfActor.printSchema()
    dfPushEvents.show(20)
    topTenUser.show(10)
    topTenRepo.show(10) //TopTenRepo

    val joinCondition = df2.col("copyID") === df.col("actor.id")
    val joinedDataFrame = df.join(df2, joinCondition,"inner" )


    joinedDataFrame.drop("copyID", "copyType").show()

    // check the JSON file inside the dataOutput1.json folder in this repo
//    joinedDataFrame.write.json("/Users/s0k08m1/Downloads/dataOutput1.json")



    //    val newDf = dfPushEvents.withColumn("actor", struct(col("actor.*"), lit("").as("activityLevel")))

//    val copydf2 = newDf.groupBy(newDf("actor.id"), newDf("type")).count().orderBy(col("count").desc)
//      .withColumn("actor.activityLevel",
//        when(col("count") >= 0 &&  col("count") <= 2, "InActive")
//          .when(col("count") > 2 && col("count") <= 5, "ModerateActive")
//          .otherwise("MostActive"))
//
//
//    copydf2.printSchema()

  }
}