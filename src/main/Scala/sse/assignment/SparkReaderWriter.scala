package sse.assignment

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{current_timestamp, date_format}

import java.io.{FileNotFoundException, IOException}
import java.util.Properties
import scala.io.Source
/*
   The read method allows the factory object to interact with various file readings depending on the file type
   Spark is a common object between all readers
  */
trait SparkReaderWriter extends App{

  val logger = Logger.getLogger(getClass.getName)

  // Alternatively we can use a scala case class to enforce the schema
  val user_event_schema = StructType {
    Array(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("value", StringType),
      StructField("timestamp", LongType)
    )
  }

  val spark = SparkSession.builder()
    .master("local")
    .appName("user events")
    .getOrCreate();

  def readFile(user_event_schema:StructType, prop:Properties):DataFrame={
    spark.read.option("header", true).schema(user_event_schema).csv(prop.getProperty("input_file_path"))
  }

  // Here for the demostration purpose I wrote the output to a json file .
  def saveToFile(df:DataFrame, path:String)={
    df
      .write
      .mode("overwrite")
      .json(path)

  }

  // Flushing the output data to a hive table partitioned on daily data
  // ToDo: we can consider adding other columns to the partition key depending on the downstream use cases
  // ToDo: If the above partitioning is not enough, buckting would be another option to optimize it further
  // Ultimately our goal would be to find a right balance between the optimum read & write while deciding on the partitioning
  def saveToHive(df:DataFrame)={
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict ")
    df.withColumn("date",date_format(current_timestamp(),"yyyyMMdd"))
    df
      .withColumn("date",date_format(current_timestamp(),"yyyyMMdd"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("date")
      .saveAsTable("user_event_settings")
  }

  def load_properties(config_path:String): Properties ={

    val properties: Properties = new Properties()
    try
    {
      val source = Source.fromFile(config_path)
      properties.load(source.bufferedReader())
      if (! scala.reflect.io.File(properties.getProperty("input_file_path")).exists ){
        throw new IOException("")
      }

    }
    catch
    {
      case ex:IOException =>{
        throw new IOException("Input file not found")
      }
      case ex:Exception => {
        throw new FileNotFoundException("Properties file cannot be loaded")}
    }
    return properties
  }

}
