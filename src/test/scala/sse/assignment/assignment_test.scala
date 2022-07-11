package sse.assignment
import org.scalatest.flatspec.AnyFlatSpec

class assignment_test extends AnyFlatSpec with SparkReaderWriter {

  "The CSV file" should " have 6 records with 4 columns" in {
    val prop = load_properties("config/config.properties")
    val df = readFile(user_event_schema, prop)
    assert(df.count() === 6)
    assert(df.columns.length === 4)

  }

  "Id -> 1 in the aggregated output" should " have two settings 1. {“notification”: “false”} and 2. {“background”: “true”}" in {
    val prop = load_properties("config/config.properties")
    val df = readFile(user_event_schema, prop)
    val df_process = assignment.process(df)
    assert(df_process.where("id=1 and settings like '%notification%' ").collect()(0)(1)==Map("notification"->"false"))
    assert(df_process.where("id=1 and settings like '%background%' ").collect()(0)(1)==Map("background"->"true"))
  }

  "Test for null key" should " have unknown as a key in the output" in {
    // Pass data/user_events_with_null_key.csv as an input file to the config file
    // This covers one of the important corner cases to handle any potential null keys
    val prop = load_properties("config/config.properties")
    val df = readFile(user_event_schema, prop)
    val df_process = assignment.process(df)
    assert(df_process.where("name=='unknown'").count() === 1)

  }

}
