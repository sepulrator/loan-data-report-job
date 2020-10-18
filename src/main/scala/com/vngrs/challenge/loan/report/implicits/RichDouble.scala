package com.vngrs.challenge.loan.report.implicits

import org.apache.spark.sql.{DataFrame, SaveMode}

trait RichDouble {

  implicit class DoubleExtension(double: Double) {

    def asK = double.toInt/1000

  }

}
