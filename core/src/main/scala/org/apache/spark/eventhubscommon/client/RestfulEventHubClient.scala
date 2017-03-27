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

package org.apache.spark.eventhubscommon.client

import java.time.Duration

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.xml.XML

import com.microsoft.azure.servicebus.SharedAccessSignatureTokenProvider
import scalaj.http.{Http, HttpResponse}

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
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
private[spark] class RestfulEventHubClient(
    eventHubNamespace: String,
    numPartitionsEventHubs: Map[String, Int],
    consumerGroups: Map[String, String],
    policyKeys: Map[String, Tuple2[String, String]],
    threadNum: Int) extends EventHubClient with Logging {

  private val RETRY_INTERVAL_SECONDS = Array(2, 4, 8, 16)

  // will be used to execute requests to EventHub
  import org.apache.spark.eventhubscommon.Implicits.exec

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

  private def queryPartitionRuntimeInfo[T](
      fromResponseBodyToResult: String => T, retryIfFail: Boolean):
      Option[Map[EventHubNameAndPartition, T]] = {
    val futures = new ListBuffer[Future[(EventHubNameAndPartition, T)]]
    for ((eventHubName, numPartitions) <- numPartitionsEventHubs;
         partitionId <- 0 until numPartitions) {
      futures += Future {
        var retryTime = 0
        var successfullyFetched = false
        var response: HttpResponse[String] = null
        val ehNameAndPartition = EventHubNameAndPartition(eventHubName, partitionId)
        while (!successfullyFetched) {
          logDebug(s"start fetching latest offset of $ehNameAndPartition")
          val urlString = fromParametersToURLString(eventHubName, partitionId)
          response = Http(urlString).
            header("Authorization",
              createSasToken(eventHubName,
                policyName = policyKeys(eventHubName)._1,
                policyKey = policyKeys(eventHubName)._2)).
            header("Content-Type", "application/atom+xml;type=entry;charset=utf-8").
            timeout(connTimeoutMs = 3000, readTimeoutMs = 30000).asString
          if (response.code != 200) {
            if (!retryIfFail || retryTime > RETRY_INTERVAL_SECONDS.length - 1) {
              val errorInfoString = s"cannot get latest offset of" +
                s" $ehNameAndPartition, status code: ${response.code}, ${response.headers}" +
                s" returned error:" +
                s" ${response.body}"
              logError(errorInfoString)
              throw new Exception(errorInfoString)
            } else {
              Thread.sleep(1000 * RETRY_INTERVAL_SECONDS(retryTime))
              retryTime += 1
            }
          } else {
            successfullyFetched = true
          }
        }
        val endpointOffset = fromResponseBodyToResult(response.body)
        logDebug(s"latest offset of $ehNameAndPartition: $endpointOffset")
        (ehNameAndPartition, endpointOffset)
      }
    }
    aggregateResults(futures.toList)
  }

  override def close(): Unit = {
    // empty
  }

  override def endPointOfPartition(retryIfFail: Boolean):
      Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    queryPartitionRuntimeInfo(fromResponseBodyToEndpoint, retryIfFail)
  }
}

private[spark] object RestfulEventHubClient {
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
