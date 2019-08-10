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

val df = sqlCtx.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("combined.csv")


// Rename URL columns
val df2 = df.withColumnRenamed("URL (SEE http://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)", "URL")

val drop_list = List("SALE TYPE", "SOLD DATE", "SOURCE", "MLS#", "FAVORITE", "INTERESTED")

def dropColumns(df: org.apache.spark.sql.DataFrame, cols: List[String]) = {
    df.select(df.columns.diff(cols).map(x => new Column(x)): _*)
}

val df3 = dropColumns(df2, drop_list)

// Write to csv
df3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("redfin_homes.csv")
