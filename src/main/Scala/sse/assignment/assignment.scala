package sse.assignment


import org.apache.spark.sql.expressions.UserDefinedFunction

import java.io.{FileNotFoundException, IOException}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, map, udf}

object assignment extends SparkReaderWriter {

  import spark.implicits._
  def imputeEmptyName=(name:String)=>{
    var newName =name
    if(newName==null || newName.isEmpty){
      newName = "unknown"
      logger.warn(s"The input file contians the empty names for id $newName")
    }
    newName
  }

  // Imputes the empty names, performs the required aggregation and finally returns the aggregated df
  // For performance on the window function, this spark Jira was reffered - https://issues.apache.org/jira/browse/SPARK-8638
  // Good case -> when the data is partitoned on both id and name, we can expect relatively low shuffling in spark
  // Bad case ->  when the data is partitioned on some other columns or no partions, please expect high data shuffling (IO/Network overhead) on spark.
  // The input files must be stored in the distributed file or no-sql system and ensure the right partitioning strategy in place in the source system.
  // For this use case, the below physical plan looks good to me,
  // TODO: however, this needs to benchmarked against the large dataset(s) before moving to QA/Stage/Prod
  // == Physical Plan ==
  //CollectLimit (8)
  //+- * Project (7)
  //   +- * Filter (6)
  //      +- Window (5)
  //         +- * Sort (4)
  //            +- Exchange (3)
  //               +- * Project (2)
  //                  +- Scan csv  (1)
  //(1) Scan csv
  //Output [4]: [id#0L, name#1, value#2, timestamp#3L]
  //Batched: false
  //Location: InMemoryFileIndex [file:/Users/cparihar/Documents/Projects/assessment_09062022/data/user_events.csv]
  //ReadSchema: struct<id:bigint,name:string,value:string,timestamp:bigint>
  //(2) Project [codegen id : 1]
  //Output [4]: [id#0L, name#1, timestamp#3L, map(name#1, value#2) AS settings#12]
  //Input [4]: [id#0L, name#1, value#2, timestamp#3L]
  //(3) Exchange
  //Input [4]: [id#0L, name#1, timestamp#3L, settings#12]
  //Arguments: hashpartitioning(id#0L, NAME#1, 200), ENSURE_REQUIREMENTS, [id=#32]
  //(4) Sort [codegen id : 2]
  //Input [4]: [id#0L, name#1, timestamp#3L, settings#12]
  //Arguments: [id#0L ASC NULLS FIRST, NAME#1 ASC NULLS FIRST, timestamp#3L DESC NULLS LAST], false, 0
  //(5) Window
  //Input [4]: [id#0L, name#1, timestamp#3L, settings#12]
  //Arguments: [row_number() windowspecdefinition(id#0L, NAME#1, timestamp#3L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#18], [id#0L, NAME#1], [timestamp#3L DESC NULLS LAST]
  //(6) Filter [codegen id : 3]
  //Input [5]: [id#0L, name#1, timestamp#3L, settings#12, rnk#18]
  //Condition : (isnotnull(rnk#18) AND (rnk#18 = 1))
  //(7) Project [codegen id : 3]
  //Output [2]: [cast(id#0L as string) AS id#24, cast(settings#12 as string) AS settings#25]
  //Input [5]: [id#0L, name#1, timestamp#3L, settings#12, rnk#18]
  //(8) CollectLimit
  //Input [2]: [id#24, settings#25]
  //Arguments: 11
  // Here I haven't changed the default shuffle partition, however, we can tune it using spark.sql.shuffle.partitions if required.
  def process(df:DataFrame): DataFrame ={

    spark.udf.register("imputeEmptyName", imputeEmptyName)
    spark.udf.register("strlen", (s: String) => if (s==null || s.isEmpty) "unknown" else s)


    df.createOrReplaceTempView("user_evt")
    val df_impute = spark.sql("select id, imputeEmptyName(name) as name, value, timestamp from user_evt")


    val df_settings = df_impute.select("*")
      .withColumn("settings", map(col("name"), col("value")))
    df_settings.createOrReplaceTempView("user_evt_with_settings")


    val df_agg = spark.sql(
      """
        |SELECT id, settings, name
        |FROM   (SELECT *,
        |               Row_number()
        |                 OVER(
        |                   partition BY id, NAME
        |                   ORDER BY timestamp DESC) AS rnk
        |        FROM   user_evt_with_settings) tab
        |WHERE  rnk = 1; """.stripMargin)
    df_agg

  }

  override def main(args: Array[String]): Unit = {

    if(args.length==0)
      {
        throw new Exception("The configuration file is missing")
      }

    val prop = load_properties(args(0))

    try {
      val df = readFile(user_event_schema, prop)
      val df_agg = process(df)
      //saveToFile(df_agg, prop.getProperty("output_file_path") )
      saveToHive(df_agg)
    }
      catch{
        case e: FileNotFoundException => logger.error("Couldn't find that file.")
        case e: IOException => logger.error("Had an IOException trying to read that file")
        case e: Exception => logger.error(s"Exception occured while processing the dataframe - ${e.toString}")

      }
  }
}
