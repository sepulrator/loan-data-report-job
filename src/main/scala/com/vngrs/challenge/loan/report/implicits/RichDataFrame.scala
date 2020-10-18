package com.vngrs.challenge.loan.report.implicits

import org.apache.spark.sql.{DataFrame, SaveMode}

trait RichDataFrame {

  implicit class DataFrameExtension(df: DataFrame) {

    def saveAsCsvTo(path: String,
                    header: Boolean = true,
                    saveMode: SaveMode = SaveMode.Overwrite): Unit =
      df.coalesce(1)
        .write
        .option("header", header.toString)
        .mode(saveMode)
        .csv(path)

  }

}
