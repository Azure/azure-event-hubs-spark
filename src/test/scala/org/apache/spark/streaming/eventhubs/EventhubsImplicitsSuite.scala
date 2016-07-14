package org.apache.spark.streaming.eventhubs

import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.receiver.ReceiverSupervisor
import org.scalatest.mock.MockitoSugar

class EventhubsImplicitsSuite extends TestSuiteBase with org.scalatest.Matchers
with MockitoSugar {
  var ehClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  var executorMock: ReceiverSupervisor = _
  val ehParams = Map[String, String] (
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4",
    "eventhubs.checkpoint.dir" -> "checkpointdir",
    "eventhubs.checkpoint.interval" -> "1000"
  )

  test("StreamingContext can be implicitly converted to eventhub streaming context") {
    val ssc = new StreamingContext(master, framework, batchDuration)

    import org.apache.spark.streaming.eventhubs.Implicits._

    val stream = ssc.unionedEventHubStream(ehParams)
    val stream2 = ssc.eventHubStream(ehParams, "0")
  }
}
