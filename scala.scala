######################################################################################## Reduce logging:
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

######################################################################################## Spark DF:
//val csv = sc.textFile("hdfs:///user/vzivkovic/e3/test/test.csv")
val csv = sc.textFile("hdfs:///user/vzivkovic/e3/test/2col.csv")   
1,2
3,4
3,5
3,6
9,6
7,8
5,3
5,1
7,2
7,8

csv.collect()
// res5: Array[String] = Array(1,2, 3,4, 5,6, 7,8, 9,10)


//  SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
// Import Spark SQL data types and Row.
import org.apache.spark.sql._


//Define the schema using a case class
case class t1(a: Integer, b: Integer)
//defined class t1

// Create an RDD of csv objects 
val csv_t1 = csv.map(_.split(",")).map(el => t1(el(0).toInt, el(1).toInt) )
csv_t1.collect()
//res10: Array[t1] = Array(t1(1,2), t1(3,4), t1(9,6), t1(7,8), t1(5,3))

// change csv RDD of csv_t1 objects to a DataFrame
val csv_df = csv_t1.toDF()
// Display top 20 (by default) rows:
csv_df.show()
+-+-+
|a|b|
+-+-+
|1|2|
|3|4|
|3|5|
|3|6|
|9|6|
|7|8|
|5|3|
|5|1|
|7|2|
|7|8|
+-+-+


csv_df.printSchema()
// root
 // |-- a: integer (nullable = true)
 // |-- b: integer (nullable = true)

csv_df.select("a").distinct.collect
// res26: Array[org.apache.spark.sql.Row] = Array([1], [3], [5], [7], [9])
csv_df.select("a").distinct.count

// Find number of distinct values in column 1
csv_df.groupBy("a").count.show
+-+-----+
|a|count|
+-+-----+
|1|      1|
|3|      3|
|5|      2|
|7|      3|
|9|      1|
+-+-----+

// Find number of distinct values based on column 1 and 2 combination
csv_df.groupBy("a", "b").count.show
+-+-+-----+
|a|b|count|
+-+-+-----+
|5|1|      1|
|5|3|      1|
|1|2|      1|
|7|2|      1|
|7|8|      2|
|3|4|      1|
|3|5|      1|
|3|6|      1|
|9|6|      1|
+-+-+-----+

// What's the min number of elements from column B per item in column A? What's the average? What's the max? 
csv_df.groupBy("a", "b").count.agg(min("count"), avg("count"), max("count")).show
+----------+------------------+----------+
|MIN(count)|        AVG(count)  |MAX(count)|
+----------+------------------+----------+
|         1    |1.11111111111112|             2|
+----------+------------------+----------+

// Show all rows where A>5:
csv_df.filter("a > 5").show

########################################################################################

$command = q[hive -e  "use e3; drop table fd_quality_4ww; create table  fd_quality_4ww  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' as   select seq_id, result, UNIX_TIMESTAMP(start_time) as unix_time from e3.f6_uva_hist  where  result is not null and (category like 'InternalCritical'  or  category like 'Area') and strategy like 'FD_%' and (year=2016 )  and  start_time > '] . $start_date_time . q['  and  start_time < '] . $end_date_time . q[' ; " ];

print "Running Command: \n\n|$command|\n";
$start = time();
#system($command);
$end = time(); $run_time = $end - $start; $run_time = nearest(.0001, $run_time);  print "Run time: |$run_time|\n\n";


$command = q(hive -e  "INSERT OVERWRITE LOCAL DIRECTORY '/etlstage/Data/test/fd_quality_raw_4ww' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   select seq_id, result, unix_time  from e3.fd_quality_4ww;");


$command = q[hive -e  "use e3; INSERT OVERWRITE LOCAL DIRECTORY '/etlstage/Data/test/fd_quality_raw_4ww' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   select  seq_id, result, UNIX_TIMESTAMP(start_time) as unix_time from e3.f6_uva_hist  where  result is not null and (category like 'InternalCritical'  or  category like 'Area') and strategy like 'FD_%' and (year=2016 )  and  start_time > '] . $start_date_time . q['  and  start_time < '] . $end_date_time . q[' ; " ];

hdfs:///user/vzivkovic/e3/f6/ees_fd_limits/ees_fd_limits.csv


val textFile = sc.textFile("/home/vzivkovi/data/test/test.txt")
val textFile = sc.textFile("hdfs:///user/vzivkovic/e3/f6/ees_fd_limits/ees_fd_limits.csv")

textFile.first()

###########  Parse CSV file  ###########
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD

val crimeFile = sc.textFile("hdfs:///user/vzivkovic/e3/f6/ees_fd_limits/ees_fd_limits.csv")
val crimeData = sc.textFile(crimeFile).cache()


#############
val lim_file = sc.textFile("hdfs:///user/vzivkovic/e3/f6/ees_fd_limits/ees_fd_limits.csv")
val csv = sc.textFile("hdfs:///user/vzivkovic/e3/test/test.csv")

val data = lim_file.map(line => line.split(",").map(elem => elem.trim))

val idx = header.split(",").indexOf(columnName)

val data = csv.map(line => line.split(","))

#####################
/user/vzivkovic/e3/f6/test/cars.csv


hdfs:///user/vzivkovic/e3/f6/test/cars.csv

CREATE TABLE cars
USING com.databricks.spark.csv
OPTIONS (path "hdfs:///user/vzivkovic/e3/f6/test/cars.csv", header "true", inferSchema "true")

.load("cars.csv")
.load("hdfs:///user/vzivkovic/e3/f6/test/cars.csv")
#####################

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
.format("com.databricks.spark.csv")
.option("header", "true") // Use first line of all files as header
.option("inferSchema", "true") // Automatically infer data types
.load("hdfs:///user/vzivkovic/e3/f6/test/cars.csv")
	
	
	
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("cars.csv")

#################################################      Working:       ###########################################
val textFile = sc.textFile("hdfs:///user/vzivkovic/e3/test/numbers.csv")
textFile.first()
textFile.foreach(println)
textFile.take(3).foreach(println)
textFile.collect().foreach(println)

val csv = sc.textFile("hdfs:///user/vzivkovic/e3/test/numbers.csv")
csv.take(3)
csv.first
csv.foreach(println)

val file = sc.textFile("/user/vzivkovic/test/unsorted.csv")
val file = sc.textFile("hdfs:///user/vzivkovic/e3/test/unsorted.csv")
#################################################      Testing:       ###########################################
rdd.coalesce(1).saveAsTextFile()
Window.partitionBy($"hour").orderBy($"TotalValue".desc).foreach(println)


val textFile = sc.textFile("hdfs:///user/vzivkovic/e3/test/numbers.csv")
                 .map( _.split(",",-1) match { case Array(col1, col2) => (col1, col2) })
                 .sortBy(_._1)
                 .map(_.1+","+_._2)
                 .saveAsTextFile("hdfs:///user/vzivkovic/e3/test/sorted_numbers.csv")
				 
				 
val file = sc.textFile("/user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2) => (col1, col2) }).sortBy(_._1).map(_._1+","+_._2).saveAsTextFile("hdfs:///user/vzivkovic/e3/test/sorted_numbers.csv")			 

val file = sc.textFile("hdfs:///user/vzivkovic/test/unsorted.csv")
val file = sc.textFile("/user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) })
val file = sc.textFile("/user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) }).sortBy(_._1)
val file = sc.textFile("/user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) }).sortBy(_._1).map(_.1+","+_._2+","+._3)
val file = sc.textFile("/user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) }).sortBy(_._1).map(_.1+","+_._2+","+._3).saveAsTextFile("hdfs:///user/vzivkovic/test/sorted_numbers3.csv")


val file = sc.textFile("hdfs:///user/vzivkovic/test/unsorted.csv")
.map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) })
.sortBy(_._1).count()
.map(_.1+","+_._2+","+._3)
.saveAsTextFile("hdfs:///user/vzivkovic/test/sorted_numbers3.csv")
				 
val file = sc.textFile("hdfs:///user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) }).sortBy(_._1).map(_._1+",")


sc.textFile("hdfs:///user/vzivkovic/test/unsorted.csv") 
.map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) }).take(2)



val textFile = sc.textFile("hdfs:///user/vzivkovic/test/unsorted.csv").count()
val textFile = sc.textFile("hdfs:///user/vzivkovic/test/1/*/unsorted.csv").count()

/user/vzivkovic/test/1/1/unsorted.csv


val file = sc.textFile("/user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) }).sortBy(_._1)
val file = sc.textFile("/user/vzivkovic/test/unsorted.csv").map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) }).sortBy(_.1).saveAsTextFile("hdfs:///user/vzivkovic/test/sorted_numbers2.csv")			 



val file = sc.textFile("hdfs:///user/vzivkovic/e3/f6/ees_fd_uva_hist/2016/01/01/00/uva.csv.gz")


WORKING:
sc.textFile("hdfs:///user/vzivkovic/test/1/*/*.csv")
.map( _.split(",",-1) match { case Array(col1, col2, col3) => (col1, col2, col3) })
.sortBy(_._1)
.saveAsTextFile("hdfs:///user/vzivkovic/test/sorted_numbers7.csv")		



sc.textFile("/apps/hive/warehouse/e3.db/f6_uva_2015_09/000000_0")
.map( _.split(",",-1) match { case Array(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16) => (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16) })
.sortBy(_._1)
.saveAsTextFile("hdfs:///user/vzivkovic/test/f6_uva_2015_09_2.csv")		

sc.textFile("hdfs:///user/vzivkovic/test/1/*/*.csv")
.map( _.split(",",-1) match { case Array(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16) => (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16) }).take(2)


take(2)

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._  // needed for ordering the dataframe

val conf = new SparkConf().setAppName("Data Import from CSV")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)

val storeDf = sqlContext.read
.format("com.databricks.spark.csv")
.option("inferSchema", "true")
.load("hdfs:///user/vzivkovic/test/1/2014/file1.csv")

.load("data/file*")

    // then we sort it and write to an output
    storeDf
      .orderBy("C0", "C1")  // default column names
      .repartition(1)   // in order to have 1 output file
      .write
      .format("com.databricks.spark.csv")
      .save("data/output")
	  
	  

/user/vzivkovic/test/1/2014/file1.csv
#################################################      Not Working:       ###########################################
textFile.collect(2).foreach(println)
textFile.second()
########################################################################################################


val column= sc.textFile("hdfs:///user/vzivkovic/e3/test/numbers.csv").map(_.split(",")(2)).flatMap(_.split(",")).map((_.toDouble))
textFile.foreach(println)

spark-shell --packages com.databricks:spark-csv_2.11:1.4.0 
spark-shell --packages com.databricks:spark-csv_2.10:1.4.0
 
#####################  Working:  ##################  
val auction = sc.textFile("hdfs:///user/vzivkovic/e3/test/ebay.csv").map(_.split(",")).map(p => 
Auction(p(0),p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat,p(6).toFloat,p(7),p(8).toInt )).toDF()
 
###########################################  Test:
 val ebayText = sc.textFile("ebay.csv")
 val ebayText = sc.textFile("hdfs:///user/vzivkovic/e3/test/ebay.csv")
 http://malhdppname2:8000/filebrowser/view//user/vzivkovic/e3/test/ebay.csv
 
 
 
 //define the schema using a case class
case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate: Integer, openbid: Float, price: Float, item: String, daystolive: Integer)

// create an RDD of Auction objects 
val ebay = ebayText.map(_.split(",")).map(p => Auction(p(0),p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat,p(6).toFloat,p(7),p(8).toInt ))




import scala.sys.process._
val lsResult = Seq("hadoop","fs","-ls","hdfs://sandbox.hortonworks.com/demo/").!!