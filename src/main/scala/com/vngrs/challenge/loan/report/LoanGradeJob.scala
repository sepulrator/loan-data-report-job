package com.vngrs.challenge.loan.report

import org.apache.spark.sql.{DataFrame, SparkSession}
import implicits._
import org.apache.spark.sql.functions.{avg, rtrim, trim}
import org.apache.spark.sql.types.IntegerType

class LoanGradeJob(df: DataFrame, val outputPath: String)(implicit val sparkSession: SparkSession)
  extends Job {

  import sparkSession.implicits._

  val name: String = "report_two"

  override def execute(): Unit = {
    val report2 = df
      .filter($"loan_amnt" === $"funded_amnt" && $"loan_amnt" > 1000.0)
      .withColumn("is_fully_paid", ($"loan_status" === "Fully Paid").cast(IntegerType))
      .groupBy("grade")
      .agg($"is_fully_paid".rate.as("fully_paid_rate"))

    report2.saveAsCsvTo(s"$outputPath/$name")

  }
  
}
