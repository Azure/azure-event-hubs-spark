/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.streaming.examples.receiverdstream.common

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.DataFrame

object DataFrameExtensions {

  implicit def extendedDataFrame(dataFrame: DataFrame): ExtendedDataFrame =
    new ExtendedDataFrame(dataFrame: DataFrame)

  class ExtendedDataFrame(dataFrame: DataFrame) {

    def insertToAzureSql(sqlDatabaseConnectionString: String, sqlTableName: String): Unit = {

      val tableHeader: String = dataFrame.columns.mkString(",")

        dataFrame.foreachPartition { partition =>
          val sqlExecutorConnection: Connection = DriverManager.getConnection(
            sqlDatabaseConnectionString)

          // Batch size of 1000 is used since Azure SQL database cannot insert more than 1000 rows
          // at the same time.

          partition.grouped(1000).foreach {
            group =>
              val insertString: scala.collection.mutable.StringBuilder = new StringBuilder()
              group.foreach {
                record => insertString.append("('" + record.mkString(",") + "'),")
              }
              sqlExecutorConnection.createStatement()
                .executeUpdate(f"INSERT INTO [dbo].[$sqlTableName] ($tableHeader) VALUES "
                  + insertString.stripSuffix(","))
          }

          sqlExecutorConnection.close()
        }
    }
  }
}