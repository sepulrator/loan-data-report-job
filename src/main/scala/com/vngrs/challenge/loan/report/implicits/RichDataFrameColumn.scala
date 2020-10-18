package com.vngrs.challenge.loan.report.implicits

import org.apache.spark.sql.functions.{count, round, sum, when}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

trait RichDataFrameColumn {

  implicit class DataFrameColumnExtension(column: Column) {

    // creates dynamically create case when conditions for range column
    def range(min: Double, max: Double, interval: Double): Column = {
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

    def rate: Column = round((sum(column) / count(column)) * 100, 2)

  }

}
