package com.java4u.spark

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

/*1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
2. Find all the patients who born in 2011
3. Find all the patients age
4. List patients whose last visited more than 60 days ago
5. Select patients 18 years old or younger*/
object PatientUsecase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Hello QTronic")
      .master("local[*]")
      .getOrCreate()

    val patientsDF = spark.read.format("csv").option("header",true).
      load("src/main/resources/patients.csv")
    val tottal =  patientsDF.select("patientID").count()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val todayDate = current_timestamp()



   /* patientsDF.where(col("lastVisitDate").gt("2012-01-1") && col("lastVisitDate").lt(current_timestamp()))
    .show()*/
    //patientsDF.where(year(col("dateOfBirth")) === 2011).show()
   /* patientsDF.withColumn("age",year(current_date())-
      year(col("dateOfBirth"))).show()*/

  //  patientsDF.where(datediff(current_date(),col("lastVisitDate")).gt(60)).show()
  val agedDF =  patientsDF.withColumn("age",year(current_date())-
      year(col("dateOfBirth")))
    agedDF.where(col("age").leq(18)).show()
   println(tottal)

    spark.stop()
  }

}
