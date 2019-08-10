// Type this into the command line to start the shell
// spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext

val sqlCtx = new SQLContext(sc)

import sqlCtx._
import sqlContext.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

val df = sqlCtx.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("file:///home/ajc867/SchoolHouseBlock/Ohio_Public_Schools_with_Latlong.csv")

val drop_list = List("SCHOOL TYPE", "ORGANIZATION TYPE", "ORGANIZATION CATEGORY", "STATUS", "DESIGNATED COUNTY", "WEB URL", "ORG EMAIL ADDRESS", "ORG FAX", "SUPERINTENDENT", "SUPERINTENDENT EMAIL", "SUPERINTENDENT PHONE", "TREASURER", "TREASURER EMAIL", "TREASURER PHONE", "PRINCIPAL", "PRINCIPAL EMAIL", "PRINCIPAL PHONE", "PARENT IRN", "PARENT ORGANIZATION NAME")

def dropColumns(df: org.apache.spark.sql.DataFrame, cols: List[String]) = {
    df.select(df.columns.diff(cols).map(x => new Column(x)): _*)
}

val df2 = dropColumns(df, drop_list)

val df3 = df2.dropDuplicates(List("IRN"))

// Write to csv
df3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/ajc867/SchoolHouseBlock/Ohio_Public_Schools_Cleaned")
