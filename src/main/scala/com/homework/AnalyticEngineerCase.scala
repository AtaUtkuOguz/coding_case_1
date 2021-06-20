package com.homework

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object AnalyticEngineerCase {

  val rootLogger: Logger = LogManager.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  System.setProperty("hadoop.home.dir", "C:\\bin")
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    //.enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def extract_councils(): DataFrame = {
    //Reads for England Councils
    val path = ".\\src\\resources\\data\\england_councils\\"
    val allTables: Seq[String] = Seq("district_councils.csv","london_boroughs.csv","metropolitan_districts.csv","unitary_authorities.csv")
    val allDf: Seq[DataFrame] = allTables.map(oneTable =>  readCsv(path, oneTable) )
    allDf.reduce(_ union _)

  }

  def readCsv(path:String,oneTable: String): DataFrame ={
    //This method is called in order to standardise reading methods
    import org.apache.spark.sql.functions.lit

    val pathWithSubFolder = path + oneTable
    println(s"Reading For $oneTable" )
    val df: DataFrame = spark.read.option("header",true).csv(pathWithSubFolder)
    val key: String = oneTable.replace(".csv", "").replace("_"," ")
    val key2: String = key.split(' ').map(_.capitalize).mkString(" ")

    val df2 =df.withColumn("council_type", lit(key2))
    df2.printSchema()
    df2.show()
    df2
  }

  def extract_avg_price(): DataFrame = {
    //Reads for property_avg_price.csv

    val path = ".\\src\\resources\\data\\"
    val oneTable =  "property_avg_price.csv"
    readCsv(path, oneTable)
  }

  def extract_sales_volume(): DataFrame = {
    //Reads for property_sales_volume.csv

    val path = ".\\src\\resources\\data\\"
    val oneTable =  "property_sales_volume.csv"
    readCsv(path, oneTable)
  }

  def transform(councilsDf: DataFrame, avgPriceDf: DataFrame, salesVolumeDf: DataFrame): DataFrame = {
    //Make Joins with taken dataframes and returned joinedDf

    val avgPriceDf2 = avgPriceDf.drop("council_type")
    val salesVolumeDf2 = salesVolumeDf.drop("council_type")

    val resultDf = councilsDf.join(avgPriceDf2,councilsDf("council") ===  avgPriceDf2("local_authority"),"left")
    val resultDf2 = resultDf.join(salesVolumeDf2,resultDf("council") ===  salesVolumeDf2("local_authority"),"left")
    val resultDf3 = resultDf2.select("council","county","council_type","avg_price_nov_2019","sales_volume_sep_2019")
    resultDf3.printSchema()
    resultDf3.show()
    resultDf3
  }


  def main(args: Array[String]) = {


    println("TMNL Case Started")

    val resDf = transform(extract_councils(), extract_avg_price(), extract_sales_volume())
    resDf.repartition(1).write.mode("overwrite").csv(".\\src\\resources\\output\\result.csv")
    println("Finished Case Study")

  }
}
