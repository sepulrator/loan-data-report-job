package com.vngrs.challenge.loan.report

import org.apache.spark.sql.{DataFrame, SparkSession}
import implicits._
import org.apache.spark.sql.functions.{avg, rtrim, trim}
import org.apache.spark.sql.types.IntegerType

class LoanStatsJob(df: DataFrame, val outputPath: String)(implicit val sparkSession: SparkSession)
  extends Job {

  import sparkSession.implicits._

  val name: String = "report_one"

  override def execute(): Unit = {
    val report1 = df
      .withColumn("term_num", rtrim(trim($"term") , " months").cast(IntegerType))
      .withColumn("income_range", $"annual_inc".range(40000.0,100000.0, 20000.0))
      .groupBy("income_range")
      .agg(avg("loan_amnt").as("avg_loan_amnt"), avg("term_num").as("avg_term"))

    report1.saveAsCsvTo(s"$outputPath/$name")
  }
}
