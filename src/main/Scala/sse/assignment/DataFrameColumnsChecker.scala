//package sse.assignment
//import org.apache.spark.sql.DataFrame
//
//case class MissingDataFrameColumnsException(smth: String) extends Exception(smth)
//
///*
//  A validator class that checks whether the number of columns are present in the dataframe or not
//  if found missing columns, a custom exception is thrown.
// */
//class DataFrameColumnsChecker(df: DataFrame, requiredColNames: Seq[String]) {
//
//  val missingColumns = requiredColNames.zip(df.columns.toSeq).filter(p=>p._1.compareTo(p._2)>0)
//
//  def missingColumnsMessage(): String = {
//    val missingColNames = missingColumns.mkString(", ")
//    val allColNames = df.columns.mkString(", ")
//    s"The [${missingColNames}] columns are not included in the DataFrame with the following columns [${allColNames}]"
//  }
//
//  def validatePresenceOfColumns(): Unit = {
//    if (missingColumns.nonEmpty) {
//      throw MissingDataFrameColumnsException(missingColumnsMessage())
//    }
//  }
//
//}