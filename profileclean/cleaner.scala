val schooldata= sc.textFile("hdfs://dumbo/user/ajc867/schoolhouseblock/1718ohio.csv")
val columns = schooldata.map(line => line.split(',')).collect
// Get tuple of elements outlined in schema order.
val coltups = columns.map(line => (line(1), line(3), line(5), line(6), line(7), line(8), line(9), line(10)))
// Drop NaNs.
val col_valid_nums = coltups.filter(tup => tup._3 != "NA" && tup._4 != "NA" && tup._5 != "NA" && tup._6 != "NA" && tup._7 != "NA" && tup._8 != "NA")
sc.parallelize(col_valid_nums).saveAsTextFile("hdfs://dumbo/user/ajc867/schoolhouseblock/1718ohio_cleaned")

