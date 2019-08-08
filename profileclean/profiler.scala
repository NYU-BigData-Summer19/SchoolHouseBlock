val schooldata= sc.textFile("hdfs://dumbo/user/ajc867/schoolhouseblock/1718ohio.csv")
val columns = schooldata.map(line => line.split(','))
// Get district name string min and max length.
val dname_lens = columns.map(record => record(1).length)
println(dname_lens.min)
println(dname_lens.max)
// Get school names string min and max length.,
val sname_lens = columns.map(record => record(3).length)
println(sname_lens.min)
println(sname_lens.max)
