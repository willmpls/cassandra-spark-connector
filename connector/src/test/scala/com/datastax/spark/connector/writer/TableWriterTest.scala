/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.spark.connector.writer

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types._
import org.scalatest.{FlatSpec, Matchers}

class TableWriterTest extends FlatSpec with Matchers {

  val keyColumn = ColumnDef("key", PartitionKeyColumn, IntType)
  val setColumn = ColumnDef("set_col", RegularColumn, SetType(TextType))
  val listColumn = ColumnDef("list_col", RegularColumn, ListType(TextType))
  val valueColumn = ColumnDef("value", RegularColumn, TextType)

  val tableDef = TableDef(
    "test_ks",
    "test_table",
    Seq(keyColumn),
    Seq.empty,
    Seq(setColumn, listColumn, valueColumn)
  )

  "TableWriter.queryTemplateUsingInsert" should "include TTL option when specified" in {
    val writeConf = WriteConf(ttl = TTLOption.constant(100))
    val writer = TableWriter[(Int, Set[String], List[String], String)](
      null, tableDef, AllColumns, writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingInsert should include ("USING TTL 100")
  }

  it should "include timestamp option when specified" in {
    val writeConf = WriteConf(timestamp = TimestampOption.constant(12345L))
    val writer = TableWriter[(Int, Set[String], List[String], String)](
      null, tableDef, AllColumns, writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingInsert should include ("USING TIMESTAMP 12345")
  }

  it should "include both TTL and timestamp options when both are specified" in {
    val writeConf = WriteConf(
      ttl = TTLOption.constant(100),
      timestamp = TimestampOption.constant(12345L))
    val writer = TableWriter[(Int, Set[String], List[String], String)](
      null, tableDef, AllColumns, writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingInsert should include ("USING TTL 100 AND TIMESTAMP 12345")
  }

  it should "include per-row TTL placeholder when specified" in {
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"))
    val writer = TableWriter[(Int, Set[String], List[String], String)](
      null, tableDef, AllColumns, writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingInsert should include ("USING TTL :ttl_column")
  }

  it should "include per-row timestamp placeholder when specified" in {
    val writeConf = WriteConf(timestamp = TimestampOption.perRow("ts_column"))
    val writer = TableWriter[(Int, Set[String], List[String], String)](
      null, tableDef, AllColumns, writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingInsert should include ("USING TIMESTAMP :ts_column")
  }

  "TableWriter.queryTemplateUsingUpdate" should "include TTL option when using collection append" in {
    val writeConf = WriteConf(ttl = TTLOption.constant(100))
    val writer = TableWriter[(Int, Set[String])](
      null, tableDef, SomeColumns("key", "set_col".append), writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingUpdate should include ("USING TTL 100")
  }

  it should "include timestamp option when using collection append" in {
    val writeConf = WriteConf(timestamp = TimestampOption.constant(12345L))
    val writer = TableWriter[(Int, Set[String])](
      null, tableDef, SomeColumns("key", "set_col".append), writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingUpdate should include ("USING TIMESTAMP 12345")
  }

  it should "include both TTL and timestamp options when using collection append" in {
    val writeConf = WriteConf(
      ttl = TTLOption.constant(100),
      timestamp = TimestampOption.constant(12345L))
    val writer = TableWriter[(Int, Set[String])](
      null, tableDef, SomeColumns("key", "set_col".append), writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingUpdate should include ("USING TTL 100 AND TIMESTAMP 12345")
  }

  it should "include per-row TTL placeholder when using collection append" in {
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"))
    val writer = TableWriter[(Int, Set[String])](
      null, tableDef, SomeColumns("key", "set_col".append), writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingUpdate should include ("USING TTL :ttl_column")
  }

  it should "include TTL option when using list prepend" in {
    val writeConf = WriteConf(ttl = TTLOption.constant(100))
    val writer = TableWriter[(Int, List[String])](
      null, tableDef, SomeColumns("key", "list_col".prepend), writeConf, checkPartitionKey = false)

    writer.queryTemplateUsingUpdate should include ("USING TTL 100")
  }

  it should "generate proper UPDATE statement format with options" in {
    val writeConf = WriteConf(ttl = TTLOption.constant(100))
    val writer = TableWriter[(Int, Set[String])](
      null, tableDef, SomeColumns("key", "set_col".append), writeConf, checkPartitionKey = false)

    // The UPDATE should have the format: UPDATE table USING TTL ... SET ... WHERE ...
    val query = writer.queryTemplateUsingUpdate
    val usingIndex = query.indexOf("USING")
    val setIndex = query.indexOf("SET")
    val whereIndex = query.indexOf("WHERE")

    usingIndex should be > 0
    setIndex should be > usingIndex
    whereIndex should be > setIndex
  }
}
