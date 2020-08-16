package analysisreport
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object BaselineData {
  def main(args: Array[String]) {

    //spark-session configuration
    val spark =  SparkSession.builder()
                             .appName("Baseline-Analysis")
                             .master("local[*]")
                             .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Reading the US_Barley data
    val US_Barley1 =  spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Barley'!A493:B506")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Barley = US_Barley1.toDF("Crop_Year", "US_Barley_Harvest")

    // Reading the World_Barley data
    val World_Barley1 = spark.read.format("com.crealytics.spark.excel")
                                  .option("dataAddress", "'Barley'!A511:B524")
                                  .option("useHeader", "true")
                                  .option("inferSchema", "false")
                                  .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")


    val World_Barley = World_Barley1.toDF("Crop_Year","World_Barley_Harvest")

    // Inner_Join on column Crop_Year between US_Barley & World_Barley dataframe
    val InnerJoin_DF = World_Barley.join(US_Barley,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val outputBarley = InnerJoin_DF.withColumn("US_Barley_Harvest%",round(col("US_Barley_Harvest") / col("World_Barley_Harvest") * 100,2))
                                    .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))

    // Reading the US_Beef data
    val US_Beef1 =  spark.read.format("com.crealytics.spark.excel")
                              .option("dataAddress", "'Beef'!A583:B596")
                              .option("useHeader", "true")
                              .option("inferSchema", "false")
                              .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Beef_R = US_Beef1.toDF("Year","US_Slaughter1")
    val US_Beef = US_Beef_R.withColumn("US_Beef_Slaughter", regexp_replace(col("US_Slaughter1"), "--", "0")).drop(col("US_Slaughter1"))


    // Reading the World_Beef data
    val World_Beef1 = spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Beef'!A601:B614")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Beef = World_Beef1.toDF("Year","World_Beef_Slaughter")


    // Inner_Join on column Crop_Year between US_Beef & World_Beef dataframe
    val InnerJoin_Beef = World_Beef.join(US_Beef,Seq("Year"),"inner").orderBy("Year")

    val outputBeef = InnerJoin_Beef.withColumn("US_Beef_Slaughter%",col("US_Beef_Slaughter")/col("World_Beef_Slaughter")*100)


    // Reading the US_Corn data
    val US_Corn1 = spark.read.format("com.crealytics.spark.excel")
                             .option("dataAddress", "'Corn'!A637:B650")
                             .option("useHeader", "true")
                             .option("inferSchema", "false")
                             .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Corn = US_Corn1.toDF("Crop_Year","US_Corn_Harvest")

    // Reading the World_Corn data

    val World_Corn1 = spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Corn'!A673:B686")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Corn = World_Corn1.toDF("Crop_Year","World_Corn_Harvest")


    // Inner_Join on column Crop_Year between US_Corn & World_Corn dataframe
    val InnerJoin_Corn = World_Corn.join(US_Corn,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))

    val outputCorn = InnerJoin_Corn.withColumn("US_Corn_Harvest%",round(col("US_Corn_Harvest") / col("World_Corn_Harvest") * 100,2))
                                    .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))

    //Reading US Cotton Data
    val US_Cotton1 =  spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Cotton'!A529:B542")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Cotton = US_Cotton1.toDF("Crop_Year","US_Cotton_Harvest")

    //Reading World Cotton Data
    val World_Cotton1 = spark.read.format("com.crealytics.spark.excel")
                                  .option("dataAddress", "'Cotton'!A565:B578")
                                  .option("useHeader", "true")
                                  .option("inferSchema", "false")
                                  .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Cotton = World_Cotton1.toDF("Crop_Year","World_Cotton_Harvest")

    // Inner_Join on column Crop_Year between US_Cotton & World_Cotton Dataframe
    val InnerJoin_Cotton = World_Cotton.join(US_Cotton,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val outputCotton = InnerJoin_Cotton.withColumn("US_Cotton_Harvest%", round(col("US_Cotton_Harvest") / col("World_Cotton_Harvest") * 100,2))
                                        .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))


    //Reading US Pork Data
    val US_Pork1 =  spark.read.format("com.crealytics.spark.excel")
                              .option("dataAddress", "'Pork'!A421:B434")
                              .option("useHeader", "true")
                              .option("inferSchema", "false")
                              .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Pork = US_Pork1.toDF("Year","US_Pork_Slaughter")

    //Reading World Pork Data
    val World_Pork1 = spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Pork'!A457:B470")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Pork = World_Pork1.toDF("Year","World_Pork_Slaughter")

    // Inner_Join on column Crop_Year between US_Pork & World_Pork Dataframe

    val InnerJoin_Pork = World_Pork.join(US_Pork,Seq("Year"),"inner").orderBy(("Year"))


    val outputPork = InnerJoin_Pork.withColumn("US_Pork_Slaughter%",round(col("US_Pork_Slaughter")/col("World_Pork_Slaughter")*100,2))

    //Reading US Poultry Data

    val US_Poultry1 =  spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Poultry'!A674:B687")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Poultry = US_Poultry1.toDF("Year","US_Poultry")

    //Reading World Poultry Data

    val World_Poultry1 =  spark.read.format("com.crealytics.spark.excel")
                                    .option("dataAddress", "'Poultry'!A710:B723")
                                    .option("useHeader", "true")
                                    .option("inferSchema", "false")
                                    .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Poultry = World_Poultry1.toDF("Year","World_Poultry")

    // Inner_Join on column Crop_Year between US_Poultry & World_Poultry Dataframe
    val InnerJoin_Poultry = World_Poultry.join(US_Poultry,Seq("Year"),"inner").orderBy(("Year"))

    val outputPoultry = InnerJoin_Poultry.withColumn("US_Poultry%",round(col("US_Poultry")/col("World_Poultry")*100,2))


    //Reading US Rice Data
    val US_Rice1 =  spark.read.format("com.crealytics.spark.excel")
                              .option("dataAddress", "'Rice'!A727:B740")
                              .option("useHeader", "true")
                              .option("inferSchema", "false")
                              .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Rice = US_Rice1.toDF("Crop_Year","US_Rice_Harvest")

    //Reading World Rice Data
    val World_Rice1 = spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Rice'!A763:B776")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Rice = World_Rice1.toDF("Crop_Year","World_Rice_Harvest")

    // Inner_Join on column Crop_Year between US_Rice & World_Rice Dataframe

    val InnerJoin_Rice = World_Rice.join(US_Rice,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val outputRice = InnerJoin_Rice.withColumn("US_Rice_Harvest%",round(col("US_Rice_Harvest") / col("World_Rice_Harvest") * 100,2))
                                   .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))

    //Reading US Sorghum Data

    val US_Sorghum1 = spark.read.format("com.crealytics.spark.excel")
                                .option("dataAddress", "'Sorghum'!A331:B344")
                                .option("useHeader", "true")
                                .option("inferSchema", "false")
                                .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Sorghum = US_Sorghum1.toDF("Crop_Year","US_Sorghum_Harvest")

    //Reading World Sorghum Data
    val World_Sorghum1 =  spark.read.format("com.crealytics.spark.excel")
                                    .option("dataAddress", "'Sorghum'!A349:B362")
                                    .option("useHeader", "true")
                                    .option("inferSchema", "false")
                                    .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Sorghum = World_Sorghum1.toDF("Crop_Year","World_Sorghum_Harvest")

    // Inner_Join on column Crop_Year between US_Sorghum & World_SorghumDataframe
    val InnerJoin_Sorghum = World_Sorghum.join(US_Sorghum,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val outputSorghum = InnerJoin_Sorghum.withColumn("US_Sorghum_Harvest%",round(col("US_Sorghum_Harvest") / col("World_Sorghum_Harvest") * 100,2))
                                         .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))

    //Reading US Soybeans Data

    val US_Soybeans1 =  spark.read.format("com.crealytics.spark.excel")
                                  .option("dataAddress", "'Soybeans'!A529:B542")
                                  .option("useHeader", "true")
                                  .option("inferSchema", "false")
                                  .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Soybeans = US_Soybeans1.toDF("Crop_Year","US_Soybeans_Harvest")

    //Reading World Soybeans Data
    val World_Soybeans1 = spark.read.format("com.crealytics.spark.excel")
                                    .option("dataAddress", "'Soybeans'!A565:B578")
                                    .option("useHeader", "true")
                                    .option("inferSchema", "false")
                                    .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Soybeans = World_Soybeans1.toDF("Crop_Year","World_Soybeans_Harvest")

    // Inner_Join on column Crop_Year between US_Soybeans & World_Soybeans ataframe
    val InnerJoin_Soybeans = World_Soybeans.join(US_Soybeans,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val outputSoybeans = InnerJoin_Soybeans.withColumn("US_Soybeans_Harvest%",round(col("US_Soybeans_Harvest") / col("World_Soybeans_Harvest") * 100,2))
                                            .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))

    //Reading US Soybeans meal Data
    val US_Soybeans_meal1 = spark.read.format("com.crealytics.spark.excel")
                                      .option("dataAddress", "'Soybean meal'!A565:B578")
                                      .option("useHeader", "true")
                                      .option("inferSchema", "false")
                                      .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Soybeans_meal = US_Soybeans_meal1.toDF("Crop_Year","US_Soybeans_meal_Harvest")

    //Reading World Soybeans meal Data
    val World_Soybeans_meal1 =  spark.read.format("com.crealytics.spark.excel")
                                          .option("dataAddress", "'Soybean meal'!A601:B614")
                                          .option("useHeader", "true")
                                          .option("inferSchema", "false")
                                          .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Soybeans_meal = World_Soybeans_meal1.toDF("Crop_Year","World_Soybeans_meal_Harvest")

    // Inner_Join on column Crop_Year between US_Soybeans_meal & World_Soybeans_meal Dataframe
    val InnerJoin_Soybeans_meal = World_Soybeans_meal.join(US_Soybeans_meal,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val outputSoybeans_meal = InnerJoin_Soybeans_meal.withColumn("US_Soybeans_meal_Harvest%",round(col("US_Soybeans_meal_Harvest") / col("World_Soybeans_meal_Harvest") * 100,2))
                                                      .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))



    //Reading US Soybeans Oil Data

    val US_Soybeans_oil1 =  spark.read.format("com.crealytics.spark.excel")
                                      .option("dataAddress", "'Soybean oil'!A565:B578")
                                      .option("useHeader", "true")
                                      .option("inferSchema", "false")
                                      .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Soybeans_oil = US_Soybeans_oil1.toDF("Crop_Year","US_Soybeans_oil_Harvest")

    //Reading World Soybeans Oil Data
    val World_Soybeans_oil1 = spark.read.format("com.crealytics.spark.excel")
                                        .option("dataAddress", "'Soybean oil'!A601:B614")
                                        .option("useHeader", "true")
                                        .option("inferSchema", "false")
                                        .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Soybeans_oil = World_Soybeans_oil1.toDF("Crop_Year","World_Soybeans_oil_Harvest")

    // Inner_Join on column Crop_Year between US_Soybeans_Oil & World_Soybeans_Oil Dataframe
    val InnerJoin_Soybeans_oil = World_Soybeans_oil.join(US_Soybeans_oil,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val OutputSoybeans_oil = InnerJoin_Soybeans_oil.withColumn("US_Soybeans_oil_Harvest%",round(col("US_Soybeans_oil_Harvest") / col("World_Soybeans_oil_Harvest") * 100,2))
                                                    .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))



    //Reading US Wheat Data

    val US_Wheat1 = spark.read.format("com.crealytics.spark.excel")
                              .option("dataAddress", "'Wheat'!A691:B704")
                              .option("useHeader", "true")
                              .option("inferSchema", "false")
                              .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val US_Wheat = US_Wheat1.toDF("Crop_Year","US_Wheat_Harvest")

    //Reading World Wheat Data
    val World_Wheat1 =  spark.read.format("com.crealytics.spark.excel")
                                  .option("dataAddress", "'Wheat'!A727:B740")
                                  .option("useHeader", "true")
                                  .option("inferSchema", "false")
                                  .load("src/main/resources/com/baseline/final/InternationalBaseline2019-Final.xls")

    val World_Wheat = World_Wheat1.toDF("Crop_Year","World_Wheat_Harvest")

    // Inner_Join on column Crop_Year between US_Wheat & World_Wheat Dataframe
    val InnerJoin_Wheat = World_Wheat.join(US_Wheat,Seq("Crop_Year"),"inner").orderBy(("Crop_Year"))


    val OutputWheat = InnerJoin_Wheat.withColumn("US_Wheat_Harvest%",round(col("US_Wheat_Harvest") / col("World_Wheat_Harvest") * 100,2))
                                     .withColumn("Year",trim(expr("substring(Crop_Year,1,5)"))).drop(col("Crop_Year"))


    //Final Output

    val InnerJoin_Finaloutput = outputBarley.join(outputBeef,Seq("Year"),"inner")
                                            .join(outputCorn,Seq("Year"),"inner")
                                            .join(outputCotton,Seq("Year"),"inner")
                                            .join(outputPork,Seq("Year"),"inner")
                                            .join(outputPoultry,Seq("Year"),"inner")
                                            .join(outputRice,Seq("Year"),"inner")
                                            .join(outputSorghum,Seq("Year"),"inner")
                                            .join(outputSoybeans,Seq("Year"),"inner")
                                            .join(outputSoybeans_meal,Seq("Year"),"inner")
                                            .join(OutputSoybeans_oil,Seq("Year"),"inner")
                                            .join(OutputWheat,Seq("Year"),"inner")
                                            .select("Year","World_Barley_Harvest",
                                                    "US_Barley_Harvest%","World_Beef_Slaughter",
                                                    "US_Beef_Slaughter%","World_Corn_Harvest",
                                                    "US_Corn_Harvest%","World_Cotton_Harvest",
                                                    "US_Cotton_Harvest%","World_Pork_Slaughter",
                                                    "US_Pork_Slaughter%","World_Poultry","US_Poultry%",
                                                    "World_Rice_Harvest","US_Rice_Harvest%",
                                                    "World_Sorghum_Harvest","US_Sorghum_Harvest%",
                                                    "World_Soybeans_Harvest","US_Soybeans_Harvest%",
                                                    "World_Soybeans_meal_Harvest","US_Soybeans_meal_Harvest%",
                                                    "World_Soybeans_oil_Harvest","US_Soybeans_oil_Harvest%",
                                                    "World_Wheat_Harvest","US_Wheat_Harvest%")
                                           .sort("Year")

    InnerJoin_Finaloutput.show()
    InnerJoin_Finaloutput.coalesce(1).write.mode("overwrite").option("header","true")
                         .csv("target/FinalOutput/baselineOutput")

  }

}
