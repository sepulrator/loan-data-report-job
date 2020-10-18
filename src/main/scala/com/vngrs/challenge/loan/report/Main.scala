package com.vngrs.challenge.loan.report

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val basePath            = if (args.size > 0) args(0) else "s3://loan-data-bucket" // bucket name for s3
    val loanDataPath        = if (args.size > 1) s"${basePath}/${args(1)}" else s"$basePath/*/*/*/*/*.gz"
    val loanDataSchemaPath  = if (args.size > 2) s"${basePath}/${args(2)}" else s"$basePath/schema/schema.csv"

    implicit val spark = SparkSession.builder
      .master("local")
      .appName("loan-data-report-generator")
      .getOrCreate()

    // load schema from bucket since there is no header in uploaded multipart loan data
    val loanDataSchema = spark.read.option("header", "true").csv(loanDataSchemaPath).schema

    val rawData = spark.read.schema(loanDataSchema).csv(loanDataPath)

    new LoanStatsJob(rawData, basePath).execute()
    new LoanGradeJob(rawData, basePath).execute()

    spark.stop()

  }
}