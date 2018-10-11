import org.apache.spark.sql.{DataFrame, SparkSession}
//import spark.implicits


object TwitterDataNew{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
        .config("spark.master","local")
     // .config("spark.some.config.option", "local")
      .getOrCreate()

    //val df = spark.read.json("/home/raji/twitter.json") //small file from local file system

    //val df = spark.read.json("hdfs://localhost:9000/data/tweets.json") //large file from hdfs

    //val df = spark.read.json("hdfs://localhost:9000/data/tweets2.json") //large file from hdfs
    df.createOrReplaceTempView("tweet_tbl")
    val user = df.select("user")
    user.createOrReplaceTempView("user_detl")

    var runProg='Y'

    while (runProg=='Y') {

      //Menu Option

      println("****** Analytical Queries using Apache Spark ******")

      println("1 => Top 10 retweeters , number of retweets ");

      println("2 => Top 10 retweeters details -- used join ")

      println("3 => Rank on retweet partition by id , rank on create date -- used window rank ")

      println("4 => Lag 3 on retweet partition by id , lag on create date -- used window lag ")

      println("5 =>Devices(iPhone,Andriod,Mac etc) used to Tweet")

      println("6 =>Tweets count from different TimeZone , country in seperate file, county_code in seprate file ")

      println("7 =>Twees by lang , max(friendscount), avg(friendscount) , group by lang , order by lang")

      println("8 => Get Language count grouping, and followers_count grouping ")

      println("9 => Get Hashtags wordcount  ")

      println("10 => Get Description wordcount  ")

      println("Enter your choice:")

      val choice=scala.io.StdIn.readInt()

      choice match {


        case 1 =>

          top10retweeters(spark)

          println("Press Y to continue or N to exit:")
         runProg = scala.io.StdIn.readChar()

        case 2 =>

          top10rtUserDetl(spark)
          println("Press Y to continue or N to exit:")
          runProg = scala.io.StdIn.readChar()

         case 3 =>

           reTweetRankbyTime(spark)
           println("Press Y to continue or N to exit:")
           runProg = scala.io.StdIn.readChar()

        case 4 =>

          reTweetLagTime(spark)
          println("Press Y to continue or N to exit:")
          runProg = scala.io.StdIn.readChar()
          
        case 5 =>

          //1. Spark functions string,instr used
          getDeviceUsedDF(spark, df)

          println("Press Y to continue or N to exit:")

          runProg = scala.io.StdIn.readChar()



        case 6 =>

          //2. group by
          getUserTimezone(spark)
          println("Press Y to continue or N to exit:")

          runProg = scala.io.StdIn.readChar()

        case 7 =>

          //3 - lang, max(lang), avg(lang) group by lang, order by lang
          getlangAggr(spark)
          println("Press Y to continue or N to exit:")

          runProg = scala.io.StdIn.readChar()

        case 8 =>
          getUserother(spark)
          println("Press Y to continue or N to exit:")

          runProg = scala.io.StdIn.readChar()

        case 9 =>
          getHashtagWc(spark)
          println("Press Y to continue or N to exit:")

          runProg = scala.io.StdIn.readChar()

        case 10 =>
          getDescriptionWc(spark)
          println("Press Y to continue or N to exit:")

          runProg = scala.io.StdIn.readChar()


      }
    }


  }

  private def top10retweeters(spark: SparkSession): Unit = {

    import spark.implicits._

    val sr = spark.sql("select  retweeted_status.user.id rt_id , count(*) rt_count " +
      "from  tweet_tbl where retweeted_status.user.id is not null " +
      "group by rt_id order by rt_count desc").limit(10)
                                                   
    sr.createOrReplaceTempView("top_100_rt")


    sr.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/top_retweeters")
  }

  private def top10rtUserDetl(spark: SparkSession): Unit = {
     import spark.implicits._
     top10retweeters(spark)
     val sr = spark.sql("select user.id , user.name , user.location , user.followers_count , user.friends_count from " +
       " user_detl ur inner join top_100_rt rt on user.id = rt_id " )

      sr.coalesce(1) //single partition
        .write.format("com.databricks.spark.csv").mode("overwrite")
        .option("header", "true").save("/home/raji/proj2b/retweet_users_details")

  }
   private def reTweetRankbyTime(spark: SparkSession): Unit = {
      import spark.implicits._
      import org.apache.spark.sql.expressions.Window
     

      top10retweeters(spark)
      val sr = spark.sql("select retweeted_status.user.id user_id,retweeted_status.user.name name, retweeted_status.created_at rt_time , " +
        "dense_rank() over (partition by retweeted_status.user.id order by retweeted_status.created_at) as rank from " +
        " tweet_tbl tr inner join top_100_rt rt on retweeted_status.user.id = rt_id " )

      //val windowId = Window.partitionBy("user_id").orderBy("rt_time")

      //val wsr = sr.withColumn("rank", rank over windowId).show()

       sr.coalesce(1) //single partition
         .write.format("com.databricks.spark.csv").mode("overwrite")
         .option("header", "true").save("/home/raji/proj2b/rank")

   }

       private def reTweetLagTime(spark: SparkSession): Unit = {
          import spark.implicits._
          import org.apache.spark.sql.expressions.Window


          top10retweeters(spark)
      val sr = spark.sql("select retweeted_status.user.id user_id, retweeted_status.created_at rt_time , " +
        "lag(retweeted_status.created_at,3) over (partition by retweeted_status.user.id order by retweeted_status.created_at) as lag_time from " +
        " tweet_tbl tr inner join top_100_rt rt on retweeted_status.user.id = rt_id " )

           sr.coalesce(1) //single partition
             .write.format("com.databricks.spark.csv") .mode("overwrite")
             .option("header", "true").save("/home/raji/proj2b/lag")

       }





  private def getUserother(spark: SparkSession): Unit = {

    import spark.implicits._
    val sr = spark.sql("select  user.lang , count(*) from user_detl where user.lang is not null " +
      "group by user.lang")

    //sr.printSchema()
    //sr.show()
    sr.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/lang")

    val fl = spark.sql("select  user.followers_count , count(*) from user_detl where user.followers_count is not null " +
      "group by user.followers_count")
    //sr.printSchema()
    //sr.show()
    fl.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/followers")

  }

  //group by
  private def getUserTimezone(spark: SparkSession): Unit = {

    import spark.implicits._
    val sr = spark.sql("select  user.time_zone , count(*) from user_detl where user.time_zone is not null " +
      "group by user.time_zone")
    //sr.printSchema()
    //sr.show()
    sr.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite") 
      .option("header", "true").save("/home/raji/proj2b/usr_timezone")

    val src = spark.sql("select  place.country as country, count(*) from tweet_tbl where place.country is not null " +
      "group by place.country")
    //sr.printSchema()
    //sr.show()
    src.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/country")

    val srcd = spark.sql("select  place.country_code as country_code, count(*) from tweet_tbl where place.country_code is not null " +
      "group by place.country_code")
    //sr.printSchema()
    //sr.show()
    srcd.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/country_code")

  }


  private def getlangAggr(spark: SparkSession): Unit = {

    import spark.implicits._
    /*val sr = spark.sql("select  user.id, user.friends_count from user_detl where user.description is not null " )

    //sr.printSchema()
    //sr.show()
    sr.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv")
      .option("header", "true").save("/home/raji/proj2b/description")*/

    val sr = spark.sql("select user.lang, avg(user.friends_count) as avgf, max(user.friends_count) as maxf " +
      "from user_detl where user.lang is not null group by user.lang order by user.lang")

    sr.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/aggr")
  }



  //substring,instr
  private def getDeviceUsedDF(spark: SparkSession, df : DataFrame): Unit = {


    import spark.implicits._

    println("getDeviceUsedDF")

    val sr = df.withColumn("source", 'source.cast("string")).select("source")


    sr.createOrReplaceTempView("source_table")
    //sr.show()

    //val sr1 = spark.sql("select substring(source.indexOf(\">\")+1 ,source.indexOf(\"</a>\") ) as v from source_table")
    //val sr1 = spark.sql("select substring(source,  (instr(source, \">\" ) +1 ), (instr(source, \"</a\" ) - 10) ) as device_used " +
    //  " from source_table where source like '%Twitter for%'  ")

    val sr1 = spark.sql("select substring(source,  instr(source, \">\" ) +1 , instr(source, \"</a\" ) - instr(source, \">\" ) - 1 ) as device_used " +
      " from source_table where source like '%Twitter for%'  ")
    //sr1.show()
    sr1.createOrReplaceTempView("device_table")

    val ts3 = spark.sql("SELECT device_used , count(*) as count from device_table group by device_used")
    //ts3.show()


    ts3.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/deviceused")



  }

  private def getHashtagWc(spark: SparkSession): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hs = spark.sql("select  cast(entities.hashtags.text as string) as text from tweet_tbl " )

    hs.createTempView("hashTags")



    val sr = spark.sql("select  substring(text,  2 , instr(text, \"]\" ) - 2 ) as hastag_text  from hashTags ")

    val hsd = sr.withColumn("hastag_text", explode(split($"hastag_text", "[,]")))

    hsd.createTempView("hashtags_tbl")
    val src = spark.sql("select  hastag_text , count(*) wordcount from hashtags_tbl where hastag_text is not null " +
     "group by hastag_text order by wordcount desc")

    src.show()
    src.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/hashtags_wc")

  }

  private def getDescriptionWc(spark: SparkSession): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hs = spark.sql("select  user.description   as description from user_detl " )

    val hsd = hs.withColumn("description", explode(split($"description", "[ ]")))

    hsd.createTempView("Desc_tbl")
    val src = spark.sql("select  description , count(*) wordcount from Desc_tbl where description is not null " +
      "group by description order by wordcount desc")

    src.show()
    src.coalesce(1) //single partition
      .write.format("com.databricks.spark.csv").mode("overwrite")
      .option("header", "true").save("/home/raji/proj2b/description")

  }


}
