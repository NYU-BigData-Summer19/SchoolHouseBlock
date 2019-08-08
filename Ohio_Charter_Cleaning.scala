// Here are all of the transformations I performed on Ohio_Charter

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

// Here's how we load with first row as columns/schema
val df = sqlCtx.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("Ohio_Charter_Raw.csv")

// Now look for columns to drop
// I.E. ones we don't find useful, ones that don't have many values
val drop_list = List("", 
                    "Report Card Watermark 2018", 
                    "Report Card Watermark 2017", 
                    "State Designation 2012", 
                    "Value-Added Component Rating 2012", 
                    "Performance Index Score 2012", 
                    "Adequate Yearly Progress Rating 2012", 
                    "State Designation 2011", 
                    "Value-Added Component Rating 2011", 
                    "Performance Index Score 2011", 
                    "Adequate Yearly Progress Rating 2011", 
                    "AMO Grade 2018", 
                    "AMO Grade 2017", 
                    "AMO Grade 2016", 
                    "AMO Grade 2015", 
                    "AMO Grade 2014", 
                    "AMO Grade 2013", 
                    "Four Year Graduation Rate Grade 2018",
                    "Four Year Graduation Rate Grade 2017",
                    "Four Year Graduation Rate Grade 2016",
                    "Four Year Graduation Rate Grade 2015",
                    "Four Year Graduation Rate Grade 2014",
                    "Four Year Graduation Rate Grade 2013",
                    "Achievement Component Grade 2018",
                    "Achievement Component Grade 2017",
                    "Achievement Component Grade 2016",
                    "Indicators Met Grade 2018",
                    "Indicators Met Grade 2017",
                    "Indicators Met Grade 2016",
                    "Indicators Met Grade 2015",
                    "Indicators Met Grade 2014",
                    "Indicators Met Grade 2013",
                    "Progress Component Grade 2018",
                    "Progress Component Grade 2017",
                    "Progress Component Grade 2016",
                    "Performance Index Grade 2018", 
                    "Performance Index Grade 2017",
                    "Performance Index Grade 2016", 
                    "Performance Index Grade 2015", 
                    "Performance Index Grade 2014", 
                    "Performance Index Grade 2013"
                    )

def dropColumns(df: org.apache.spark.sql.DataFrame, cols: List[String]) = {
    df.select(df.columns.diff(cols).map(x => new Column(x)): _*)
}

val df2 = dropColumns(df, drop_list)
// df2.columns.size

// Drop duplicate rows based on School IRN
val df3 = df2.dropDuplicates(List("School IRN"))
// df3.count()

// The "Closed column currently has 0 or 1 values
// We can just make this "Yes" or "Now"
val df4 = df3.withColumn("isClosed", when($"Closed" === 1, "Yes").otherwise("No"))
val df5 = df4.drop("Closed")

// Rename "School Type 1" to convOrStartUp
val df6 = df5.withColumnRenamed("School Type 1", "ConvOrStartup")

// Column "School Type 2" can be cleaned
// Make all values of "Site-Based" or "Site Based" instead "Site"
// Make all values of "State Wide E-School" or "Virtual School" instead "Virtual"
// Rename the column "PhysOrVirtual"
val df7 = df6.withColumn("School Type 2", when 
        ( ($"School Type 2" === "Statewide E-School")
        or ($"School Type 2" === "Virtual School"), "Virtual")
        .otherwise("Site"))
val df8 = df7.withColumnRenamed("School Type 2", "PhysOrVirtual")

// Column "School Type 3" can be cleaned
// Make "Drop Out Prevention & Recovery" to "Drop Out Prevention and Recovery"
val df9 = df8.withColumn("School Type 3", 
    when($"School Type 3" === "Dropout Prevention & Recovery", "Dropout Prevention and Recovery")
    .when($"School Type 3" === "Dropout Prevention and Recovery", "Dropout Prevention and Recovery")
    .when($"School Type 3" === "Dropout Recovery and Special Education", "Dropout Prevention and Recovery")
    .when($"School Type 3" === "General Education", "General Education")
    .otherwise("Special Education") )
val df10 = df9.withColumnRenamed("School Type 3", "StudentType")

// Write to one csv
df9.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("Ohio_Charter_Final.csv")

/* In Hive, now we can turn this cleaned csv into a table
beeline
!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
use ac3241;

create external table OhioCharterSchools location 'Ohio_Charter_Final.csv'
Here's the command with schema:

create external table OhioCharter(
SponsorIRN string,
Sponsor string,
SchoolIRN string,
School string,
Telephone string,
Address string,
PublicDistrictLocationIRN string,
PublicDistrictLocation string,
County string,
FirstYearofOperation string,
GradeLevelsServed string,
ConvOrStartup string,
PhysOrVirtual string,
SchoolType3 string,
PercentMinority double,
PercentEconomicallyDisadvantaged double,
PercentStudentswithDisabilities double,
PercentLimitedEnglishProficient double,
AttendanceRate double,
OverallGrade2018 string,
OverallValueAddedGrade2018 string,
GiftedValueAddedGrade2018 string,
StudentswithDisabilitiesValueAddedGrade2018 string,
Lowest20PercentValueAddedGrade2018 string,
HighlyMobileValueAddedGrade2018 string,
GraduationComponentGrade2018 string,
FiveYearGraduationRateGrade2018 string,
GapClosingComponentGrade2018 string,
SuccessComponentGrade2018 string,
K3LiteracyGrade2018 string,
OverallValueAddedGrade2017 string,
GiftedValueAddedGrade2017 string,
StudentswithDisabilitiesValueAddedGrade2017 string,
Lowest20PercentValueAddedGrade2017 string,
HighlyMobileValueAddedGrade2017 string,
GraduationComponentGrade2017 string,
FiveYearGraduationRateGrade2017 string,
GapClosingComponentGrade2017 string,
SuccessComponentGrade2017 string,
K3LiteracyGrade2017 string,
OverallValueAddedGrade2016 string,
GiftedValueAddedGrade2016 string,
StudentswithDisabilitiesValueAddedGrade2016 string,
Lowest20PercentValueAddedGrade2016 string,
HighlyMobileValueAddedGrade2016 string,
GraduationComponentGrade2016 string,
FiveYearGraduationRateGrade2016 string,
GapClosingComponentGrade2016 string,
SuccessComponentGrade2016 string,
K3LiteracyGrade2016 string,
OverallValueAddedGrade2015 string,
GiftedValueAddedGrade2015 string,
StudentswithDisabilitiesValueAddedGrade2015 string,
Lowest20PercentValueAddedGrade2015 string,
HighlyMobileValueAddedGrade2015 string,
FiveYearGraduationRateGrade2015 string,
K3LiteracyGrade2015 string,
OverallValueAddedGrade2014 string,
GiftedValueAddedGrade2014 string,
StudentswithDisabilitiesValueAddedGrade2014 string,
Lowest20PercentValueAddedGrade2014 string,
FiveYearGraduationRateGrade2014 string,
OverallValueAddedGrade2013 string,
GiftedValueAddedGrade2013 string,
StudentswithDisabilitiesValueAddedGrade2013 string,
Lowest20PercentValueAddedGrade2013 string,
FiveYearGraduationRateGrade2013 string,
Latitude double,
Longitude double,
isClosed string)
row format delimited fields terminated by ','
location '/user/ac3241/Ohio_Charter_Clean';



*/

df.write().mode(SaveMode.Overwrite).saveAsTable("OHCharter");
df.write.saveAsTable("ac3241.test")
