import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}


object MyClass {
  def main(args: Array[String]) : Unit = {
    print("hello from the main method")

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate();

    spark.implicits
    //println(spark)
    //println("Spark Version : "+spark.version)

    //Data Schemas
    // Play store data schema
    val playStoreSchema = StructType(Array(
      StructField("App",StringType,false),
      StructField("Category",StringType,false),
      StructField("Rating",FloatType,false),
      StructField("Reviews",IntegerType,true),
      StructField("Size",StringType,true),
      StructField("Installs",IntegerType,false),
      StructField("Type",StringType,false),
      StructField("Price",StringType,true),
      StructField("Content",StringType,true),
      StructField("Content Rating",StringType,true),
      StructField("Genres",StringType,true),
      StructField("Last Updated",StringType,true),
      StructField("Current Ver",StringType,true),
      StructField("Android Ver", IntegerType, true)
    ))

    // Play store review data schema
    val playStoreReviewSchema = StructType(Array(
      StructField("App",StringType,false),
      StructField("Translated_Review",StringType,false),
      StructField("Sentiment",StringType,false),
      StructField("Sentiment_Polarity",FloatType,false),
      StructField("Sentiment_Subjectivity",FloatType,false)
    ))

    //Spark has a built in method to read CSV to dataframe
    val  playStore_df= spark.read.option("header",true).schema(playStoreSchema).csv("/Users/catarinaserrano/Downloads/google-play-store-apps/googleplaystore.csv")
    playStore_df.show()
    playStore_df.printSchema()
    //println("Initial count of App : "+playStore_df.count())

    val playStore_review_df = spark.read.option("header",true).schema(playStoreReviewSchema).csv("/Users/catarinaserrano/Downloads/google-play-store-apps/googleplaystore_user_reviews.csv")
    playStore_review_df.show()
    playStore_review_df.printSchema()


    //Remove duplicates: Distinct using dropDuplicates from playStore_df
    val playStore_df_dist = playStore_df.dropDuplicates("App")
    //In playStore_review_df it could appear duplicates since one person can review multiple times the same APP

    //Handle missing data
    //Remove rows without name of App

    //Filter unwanted outliers




    //Part1 From googleplaystore_user_reviews.csv create a Dataframe (df_1)
    val df_1_schema = StructType(Array(
      StructField("App",StringType,false),
      StructField("Average_Sentiment_Polarity",DoubleType,false)
    ))

    //delete Sentiment_Polarity=null, it would impact the average score, in an a wrong way, so there is no need for default value of average=0, this rows will not exist
    val playStore_review_WT_NULL_Sentiment_Polarity_df= playStore_review_df.na.drop(Seq("Sentiment_Polarity"))

    playStore_review_WT_NULL_Sentiment_Polarity_df.createOrReplaceTempView("playStore_review_view")
    val df_1 = spark.sql("SELECT App, AVG(Sentiment_Polarity) " +
      "FROM playStore_review_view " +
      "GROUP BY App")
    df_1.show()

    //Part 2 Read googleplaystore.csv as a Dataframe and obtain all Apps with a "Rating" greater or equal to 4.0 sorted in descending order.
    //Save that Dataframe as a CSV (delimiter: "§") named "best_apps.csv"

    //Delete app=null and rating =null and unwanted outliers for rating (0<=rating<=5)
    val playStore_clean_df= playStore_df.na.drop(Seq("App", "Rating")).filter(playStore_df("Rating")>=0 &&  playStore_df("Rating")<=5)
    playStore_clean_df.createOrReplaceTempView("playStore_clean_df_view")
    val playStore_best_apps_df = spark.sql("SELECT * " +
      "FROM playStore_clean_df_view " +
      "WHERE Rating>4 " +
      "ORDER BY Rating DESC")
    playStore_best_apps_df.show()

    playStore_best_apps_df.write.option("delimiter","§").csv("/Users/catarinaserrano/Downloads/google-play-store-apps/best_apps")

    /*Part 3 df_3
    App should be a unique value --> already done when removing duplicates, does not make sense to have multiple entries of the same app, it would imply
    wrong calculations
    In case of App duplicates, the column "Categories" of the resulting row should contain an array with all the possible
    categories --> duplicates removed;
§
    */







  }

}