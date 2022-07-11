//package sse.assignment
//
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.types.StructType
//
//class XmlReaderWriter(root_element:String, xsd_path:String) extends SparkReaderWriter
//{
//  // Allow the reader to read the XML files
//  // Mode is specified as FAILFAST to immediately throw any parsing issue
//  // To validate the schema for each row in the XML, an XSD file is expected.
//  // If required more options can be supplied as given here-> "https://github.com/databricks/spark-xml"
//  def read(filepath:String, schema: StructType, required_column_names:Seq[String]): DataFrame =
//  {
//    import com.databricks.spark.xml._
//    val df = spark
//      .read
//      .format("com.databricks.spark.xml")
//      .option("rowTag", root_element)
//      .option("mode", "FAILFAST")
//      .option("rowValidationXSDPath", xsd_path)
//      .schema(schema)
//      .xml(filepath)
//    validatePresenceOfColumns(df, required_column_names )
//    df
//  }
//}