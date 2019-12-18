package com.java4u.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HelloRDD {

  def main(args: Array[String]) {
    val spark = SparkSession

      .builder()
      .appName("Hello QTronic")
      .master("local[*]")
      .getOrCreate()
   val list = spark.sparkContext.parallelize(List(1, 2, 3, 4))
    val odd = list.map(_*2).filter(_%2==0)
    println("I m done")
   odd.foreach(println)

    val list1 = spark.sparkContext.parallelize(List(
      "Learning Hadoop","Learning scala","Learning Spark","Learning Cassandra"
    ))
    list1.flatMap(_.split(" ")).map((_,1)).reduceByKey((x,y)=>x+y).foreach(println)

    spark.stop()
  }
}

/*
problem 1
csv format, patientID,name,dateOfBirth,lastVisitDate
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20
1003,Ali,2011-01-30,2012-10-21
Accomplish following activities.
1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
2. Find all the patients who born in 2011
3. Find all the patients age
4. List patients whose last visited more than 60 days ago
5. Select patients 18 years old or younger*/