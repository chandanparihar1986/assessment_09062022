//package sse.assignment
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.types.StructType
//
//class JsonReaderWriter(root_element:String) extends SparkReaderWriter
//{
//  // Allow the reader to read the JSON files
//  // This class assumes and handles cases for one level of nested columns
//  // if more than one level of nesting is required, the validation rules must be changed or
//  // in other words requires a bit of code refactoring.
//  // Apart from handling one level nested columns, this also covers non-nested json records.
//  def read(filepath:String, schema: StructType, required_column_names:Seq[String]): DataFrame =
//  {
//    val df = spark
//      .read.option("multiline","true")
//      .schema(schema)
//      .json(filepath)
//
//    if(root_element != ""){
//      validatePresenceOfColumns(df.select(s"${root_element}.*"), required_column_names )
//    }
//    else{
//      validatePresenceOfColumns(df, required_column_names )
//    }
//
//    df
//  }
//
//}