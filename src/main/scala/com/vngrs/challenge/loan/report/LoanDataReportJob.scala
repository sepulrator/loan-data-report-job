package com.vngrs.challenge.loan.report

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.vngrs.challenge.loan.report.Util._

object LoanDataReportJob {

  def main(args: Array[String]): Unit = {

    val basePath       = if (args.size > 0) args(0) else "s3://loan-data-bucket" // bucket name for s3
    val loanDataPath   = if (args.size > 1) s"""${basePath}/${args(1)}""" else s"""${basePath}/*/*/*/*/*.gz"""
    val report1Path    = s"""${basePath}/report_one"""
    val report2Path    = s"""${basePath}/report_two"""
    val loanDataSchemaPath = s"""${basePath}/schema/schema.csv"""

    val spark = SparkSession.builder
      .appName("loan-data-report-generator")
      .getOrCreate()

    import spark.implicits._
    // load schema from bucket since there is no header in uploaded multipart loan data
    val loanDataSchema = spark.read.option("header", "true").csv(loanDataSchemaPath).schema

    val rawData = spark.read.schema(loanDataSchema).csv(loanDataPath)

    // generate report1
    val report1 = rawData
      .withColumn("term_num", rtrim(trim($"term") , " months").cast(IntegerType))
      .withColumn("income_range", rangeColumn($"annual_inc", 40000.0,100000.0, 20000.0))
      .groupBy("income_range")
      .agg(avg("loan_amnt").as("avg_loan_amnt"), avg("term_num").as("avg_term"))

    report1.saveCsvTo(report1Path)

    // generate report2
    val report2 = rawData
      .filter($"loan_amnt" === $"funded_amnt" && $"loan_amnt" > 1000.0)
      .withColumn("is_fully_paid", ($"loan_status" === "Fully Paid").cast(IntegerType))
      .groupBy("grade")
      .agg(rate($"is_fully_paid").as("fully_paid_rate"))

    report2.saveCsvTo(report2Path)

    spark.stop()

  }
}