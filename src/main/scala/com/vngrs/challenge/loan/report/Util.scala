package com.vngrs.challenge.loan.report

import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions.{count, round, sum, when}

object Util {

  implicit class RichDouble(d: Double) {
    def asK = d.toInt/1000
  }

  implicit class RichDataFrame(df: DataFrame) {
    def saveCsvTo(path: String) = df.coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path)
  }

  // creates dynamically create case when conditions for range column
  def rangeColumn(column: Column, min: Double, max: Double, interval: Double): Column = {
    // lt min condition
    var incRange = when(column lt min, s"""<${min.asK}K""")

    //range intervals
    for (i <- min until max by interval) yield {
      val cond = column lt (i + interval)
      val text = s"""${i.asK}-${(i + interval).asK}K"""
      incRange = incRange.when(cond, text)
    }
    //otherwise gt max condition
    incRange.otherwise(s""">${max.asK}K""")
  }

  def rate(col: Column) = round((sum(col) / count(col)) * 100, 2)

}
