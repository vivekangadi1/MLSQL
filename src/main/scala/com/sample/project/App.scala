package com.sample.project



import org.apache.spark.sql.types.DateType
import org.apache.spark.sql._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.functions.countDistinct

import scala.collection.mutable
import scala.util.parsing.json.JSONObject


/**
 * Hello world!
 *
 */
case class columnName(
                       existing_col_name:String,
                       new_col_name : String,
                       new_data_type:String,
                       date_expression: Option[String] = None){

}

object App  {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Intro")
      .config("spark.master", "local")
      .getOrCreate
    val whereami = System.getProperty("user.dir")
    println(whereami)
    val rawData=spark.read
      .option("header","true")
      .option("nullValue","false")
      .option("inferSchema","true")
      .option("dateFormat", "DD-MM-YYYY")
      .csv(whereami+"\\problem.csv")
    rawData.printSchema()
    val process=new Process(spark);

    val step1 = process.step1(rawData)


    var newCol = new Array[columnName](3)
    val a=new columnName("name","first_name","string")
    val b=new columnName("age","total_years","integer")
    val c=new columnName("birthday","d_o_b","date",Some("dd-MM-yyyy"))

    newCol(0)=a;
    newCol(1)=b;
    newCol(2)=c;


    val step3=process.step3(step1,newCol)

    println("++++++++++++++++++++++++++++++++++=============================++++++++++++++++++++++\n",step3)



  }
}



class Process(private val spark:SparkSession){


  import spark.implicits._;

  def step1( rawData:DataFrame): DataFrame ={
    rawData.show(false)
    step2(rawData)
  }

  def step2 (rawData:DataFrame): DataFrame ={
    rawData.na.drop()
  }

  def step3(proceessData: DataFrame, z: Array[columnName]): String ={

    var newColumns :DataFrame =proceessData;
    for ( a <- 0 to z.size-1){
      newColumns = newColumns.withColumnRenamed(z(a).existing_col_name,z(a).new_col_name)
    }
    var newColumNameDate:String=null
    var newColumnDateFormat:Option[String] =null
    var newColName1 = new Array[String](z.size)
    var newColName2 = new Array[String](z.size)
    for ( a <- 0 to newColName1.size-1){
      newColName2(a)=z(a).new_col_name
      if(!z(a).new_data_type.equals("date")) {
        newColName1(a) = "cast(" + z(a).new_col_name + " as " + z(a).new_data_type + ") " + z(a).new_col_name
      }
      else{

        newColName1(a) = "cast(" + z(a).new_col_name + " as " + "string" + ") " + z(a).new_col_name
        newColumNameDate=z(a).new_col_name
        newColumnDateFormat=z(a).date_expression
        newColumnDateFormat.toString
      }
    }
     newColumns = newColumns.selectExpr(newColName1:_*)

    newColumns.show(false)

    newColumns.select(to_date(unix_timestamp(
      $"$newColumNameDate", newColumnDateFormat.toString
    ).cast("timestamp")).alias("timestamp"))

    step4(newColumns,newColName2)


  }

  def step4(newData: DataFrame, z: Array[String]):String ={
    var comedat:DataFrame=newData
    var strOut:String=""
      for(i<-0 to z.size-1){

        val a= comedat.agg(functions.approx_count_distinct(z(i)).alias("UniqueValues"),functions.collect_list(z(i)).alias("Values")).first()
        convertRowToJSON(a,z(i))
        def convertRowToJSON(row: Row,str:String): String = {
          val m = row.getValuesMap(row.schema.fieldNames)

          val rawObj=JSONObject(m)
          println(rawObj)
          var finalObj=  rawObj.obj.updated("Columns",z(i))
          strOut= JSONObject(finalObj).toString()+"\n" +strOut
          println(rawObj.obj.updated("Columns",z(i)).toString())
          strOut
        }

        comedat.groupBy(z(i)).count()
      }
    comedat.show()
    strOut
  }

}
