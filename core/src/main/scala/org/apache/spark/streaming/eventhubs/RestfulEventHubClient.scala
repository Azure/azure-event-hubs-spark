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

package org.apache.spark.streaming.eventhubs

import java.net.SocketTimeoutException
import java.time.{Duration, Instant}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.xml.XML

import com.microsoft.azure.servicebus.SharedAccessSignatureTokenProvider
import scalaj.http.{Http, HttpResponse}

import org.apache.spark.internal.Logging

/**
 * a Restful API based client of EventHub
 *
 * @param eventHubNamespace the namespace of eventhub
 * @param numPartitionsEventHubs a map from eventHub name to the total number of partitions
 * @param consumerGroups a map from eventHub name to consumer group names
 * @param policyKeys a map from eventHub name to (policyName, policyKey) pair
 * @param threadNum the number of threads used to communicate with remote EventHub
 */
private[eventhubs] class RestfulEventHubClient(
    eventHubNamespace: String,
    numPartitionsEventHubs: Map[String, Int],
    consumerGroups: Map[String, String],
    policyKeys: Map[String, Tuple2[String, String]],
    threadNum: Int) extends EventHubClient with Logging {

  private val RETRY_INTERVAL_SECONDS = Array(8, 16, 32, 64, 128)

  // will be used to execute requests to EventHub
  import Implicits.exec

  private def createSasToken(eventHubName: String, policyName: String, policyKey: String):
      String = {
    // the default value of 10 mins is hardcoded, and this method will be called for everytime when
    // a new batch is started, we may figure out whether there will be any negative impact for
    // creating a new sasToken everytime
    SharedAccessSignatureTokenProvider.generateSharedAccessSignature(
      s"$policyName", s"$policyKey",
      s"$eventHubNamespace.servicebus.windows.net/$eventHubName",
      Duration.ofMinutes(10))
  }

  private def fromResponseBodyToEndpoint(responseBody: String): (Long, Long) = {
    val partitionDescription = XML.loadString(responseBody) \\ "entry" \
      "content" \ "PartitionDescription"
    ((partitionDescription \ "LastEnqueuedOffset").text.toLong,
      (partitionDescription \ "EndSequenceNumber").text.toLong)
  }

  private def fromParametersToURLString(eventHubName: String, partitionId: Int): String = {
    s"https://$eventHubNamespace.servicebus.windows.net/$eventHubName" +
      s"/consumergroups/${consumerGroups(eventHubName)}/partitions/$partitionId?api-version=2015-01"
  }

  private def aggregateResults[T](undergoingRequests: List[Future[(EventHubNameAndPartition, T)]]):
      Option[Map[EventHubNameAndPartition, T]] = {
    Await.ready(Future.sequence(undergoingRequests), 60 seconds).value.get match {
      case Success(queryResponse) =>
        Some(queryResponse.toMap.map {case (eventHubQueryKey, queryResponseString) =>
          (eventHubQueryKey, queryResponseString.asInstanceOf[T])})
      case Failure(e) =>
        e.printStackTrace()
        None
    }
  }

  private def composeQuery[T](
      retryIfFail: Boolean,
      fromResponseBodyToResult: String => T,
      nameAndPartition: EventHubNameAndPartition):
      Future[(EventHubNameAndPartition, T)] = {
    Future {
      var retryTime = 0
      var successfullyFetched = false
      var response: HttpResponse[String] = null
      val ehNameAndPartition = nameAndPartition
      val eventHubName = nameAndPartition.eventHubName
      val partitionId = nameAndPartition.partitionId
      while (!successfullyFetched) {
        logDebug(s"start fetching latest offset of $ehNameAndPartition")
        val urlString = fromParametersToURLString(eventHubName, partitionId)
        try {
          response = Http(urlString).header("Authorization",
            createSasToken(eventHubName,
              policyName = policyKeys(eventHubName)._1,
              policyKey = policyKeys(eventHubName)._2)).
            header("Content-Type", "application/atom+xml;type=entry;charset=utf-8").
            timeout(connTimeoutMs = 3000, readTimeoutMs = 30000).asString
          if (response.code != 200) {
            if (!retryIfFail || retryTime > RETRY_INTERVAL_SECONDS.length - 1) {
              val errorInfoString = s"cannot get latest offset of" +
                s" $ehNameAndPartition, status code: ${response.code}, ${response.headers}" +
                s" returned error: ${response.body}"
              logError(errorInfoString)
              throw new Exception(errorInfoString)
            } else {
              val retryInterval = 1000 * RETRY_INTERVAL_SECONDS(retryTime)
              logError(s"cannot get connect with Event Hubs Rest Endpoint for partition" +
                s" $ehNameAndPartition, retry after $retryInterval seconds")
              Thread.sleep(retryInterval)
              retryTime += 1
            }
          } else {
            successfullyFetched = true
          }
        } catch {
          case e: SocketTimeoutException =>
            e.printStackTrace()
            logError("Event Hubs return ReadTimeout with 30s as threshold, retrying...")
          case e: Exception =>
            e.printStackTrace()
            throw e
        }
      }
      val results = fromResponseBodyToResult(response.body)
      logDebug(s"latest offset of $ehNameAndPartition: $results")
      (ehNameAndPartition, results)
    }
  }

  private def queryPartitionRuntimeInfo[T](
      targetEventHubsNameAndPartitions: List[EventHubNameAndPartition],
      fromResponseBodyToResult: String => T, retryIfFail: Boolean):
      Option[Map[EventHubNameAndPartition, T]] = {
    val futures = new ListBuffer[Future[(EventHubNameAndPartition, T)]]
    if (targetEventHubsNameAndPartitions.isEmpty) {
      for ((eventHubName, numPartitions) <- numPartitionsEventHubs;
           partitionId <- 0 until numPartitions) {
        futures += composeQuery(retryIfFail, fromResponseBodyToResult,
          EventHubNameAndPartition(eventHubName, partitionId))
      }
    } else {
      for (targetNameAndPartition <- targetEventHubsNameAndPartitions) {
        futures += composeQuery(retryIfFail, fromResponseBodyToResult, targetNameAndPartition)
      }
    }
    aggregateResults(futures.toList)
  }

  override def close(): Unit = {
    // empty
  }

  /**
   * return highest offset/seq and latest enqueueTime of each partition
   */
  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubsNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    queryPartitionRuntimeInfo(targetEventHubsNameAndPartitions,
      fromResponseBodyToEndpoint, retryIfFail)
  }

  private def fromResponseBodyToEnqueueTime(responseBody: String): Long = {
    val partitionDescription = XML.loadString(responseBody) \\ "entry" \
      "content" \ "PartitionDescription"
    Instant.parse((partitionDescription \ "LastEnqueuedTimeUtc").text).getEpochSecond
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
  Option[Map[EventHubNameAndPartition, Long]] = {
    queryPartitionRuntimeInfo(targetEventHubNameAndPartitions,
      fromResponseBodyToEnqueueTime, retryIfFail)
  }
}

private[eventhubs] object RestfulEventHubClient {
  def getInstance(eventHubNameSpace: String, eventhubsParams: Map[String, Map[String, String]]):
  RestfulEventHubClient = {
    new RestfulEventHubClient(eventHubNameSpace,
      numPartitionsEventHubs = {
        eventhubsParams.map { case (eventhubName, params) => (eventhubName,
          params("eventhubs.partition.count").toInt)
        }
      },
      consumerGroups = {
        eventhubsParams.map { case (eventhubName, params) => (eventhubName,
          params("eventhubs.consumergroup"))
        }
      },
      policyKeys = eventhubsParams.map { case (eventhubName, params) => (eventhubName,
        (params("eventhubs.policyname"), params("eventhubs.policykey")))
      },
      threadNum = 15)
  }
}
