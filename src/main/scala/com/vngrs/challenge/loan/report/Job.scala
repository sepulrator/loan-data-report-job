package com.vngrs.challenge.loan.report

import org.apache.spark.sql.SparkSession

trait Job {

  def sparkSession: SparkSession

  def name: String

  def execute(): Unit

  def outputPath: String

}
