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

package org.apache.spark.eventhubs.common.progress

import org.apache.hadoop.fs.Path
import org.apache.spark.eventhubs.common.NameAndPartition

private[spark] object PathTools extends Serializable {

  private def combineDirectoryNames(subDirs: Seq[String]): String =
    subDirs.mkString("/")

  private def combineDirectoryNames(dirs: String*)(implicit u: DummyImplicit): String =
    dirs.mkString("/")

  def makeProgressDirectoryStr(progressDir: String, subDirNames: String*): String =
    s"$progressDir/${combineDirectoryNames(subDirNames)}"

  def makeProgressDirectoryPath(progressDir: String, subDirNames: String*): Path =
    new Path(s"$progressDir/${combineDirectoryNames(subDirNames)}")

  def makeTempDirectoryStr(progressDir: String, subDirNames: String*): String =
    s"$progressDir/${combineDirectoryNames(subDirNames)}_temp"

  def makeTempDirectoryPath(progressDir: String, subDirNames: String*): Path =
    new Path(s"$progressDir/${combineDirectoryNames(subDirNames)}_temp")

  def makeMetadataDirectoryStr(progressDir: String, subDirNames: String*): String =
    s"$progressDir/${combineDirectoryNames(subDirNames)}_metadata"

  def makeMetadataDirectoryPath(progressDir: String, subDirNames: String*): Path =
    new Path(s"$progressDir/${combineDirectoryNames(subDirNames)}_metadata")

  def makeProgressFileName(timestamp: Long): String =
    s"progress-$timestamp"

  def makeTempFileName(streamId: Int,
                       uid: String,
                       eventHubNameAndPartition: NameAndPartition,
                       timestamp: Long): String =
    s"$streamId-$uid-$eventHubNameAndPartition-$timestamp"

  def makeTempFilePath(basePath: String,
                       streamId: Int,
                       uid: String,
                       eventHubNameAndPartition: NameAndPartition,
                       timestamp: Long): Path =
    new Path(
      s"${combineDirectoryNames(basePath, makeTempFileName(streamId, uid, eventHubNameAndPartition, timestamp))}")

  def makeMetadataFileName(timestamp: Long): String = timestamp.toString
}
