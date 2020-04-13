# Receive Key-Value Pairs from Events Sent using Event Hubs Kafka Endpoint
This guide will show you how you can properly extract a Kafka key from an event which has been produced by an 
<a href="https://github.com/Azure/azure-event-hubs-for-kafka" target="_blank">Azure Event Hubs Kafka endpoint</a>.

 
## Introduction 
Azure Event Hubs supports three protocols for consumers and producers: AMQP, Kafka, and HTTPS. 
Since each one of these protocols has its own way of representing a message, it is important to ensure that the values 
within an event are correctly interpreted by the consuming application when it uses a different protocol than the producer application.
This topic has been discussedin great details <a href="https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-exchange-events-different-protocols" target="_blank">here</a>.

Since Event Hubs Spark connector uses AMPQ protocol to send and receive events, this article shows how you can properly 
extract the Kafka key-value pair from an event that has been produced by an Event Hubs Kafka endpoint.


## Kafka Value in the Event Body
The Microsoft AMQP clients represent the value of a Kafka key-value pair as an uninterpreted bag of bytes 
in the event body. Therefore, a consuming application receives a sequence of bytes from the producing application 
and can interpret that sequence as the desired type within the application code.


## Kafka Key in the SystemPropereties
The Event Hubs Spark connector represents the key of a Kafka key-value pair in the SystemProperties. When an event has been 
produced and sent using an Event Hubs Kafka endpoint, the SystemProperties of the event contains a key-value pair of 
("x-opt-kafka-key", KAFKA_KEY) where the KAFKA_KEY is exposed as a json-serialized string which represents the 
underlying serialized byte array.

Therefore, a consuming application can extract the serialized byte array from the SystemProperties (which is exposed 
in the Spark SQL schema) and interpret  that sequence as the desired type within the application code.


## A Sample of consuming Events Produced by an Event Hubs Kafka Endpoint
Assume a producer application sends messages to a Kafka-enabled Event Hub using the Java producer example code 
<a href="https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/quickstart/java" target="_blank">here</a>.
Each message contains a (key, value) pair of type (Long, String). Here is how we can extract the key and the value
in each event by using the Event Hubs Spark connector client:

```scala
import org.apache.spark.eventhubs.{EventHubsConf, EventHubsUtils, EventPosition}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.nio.ByteBuffer

def convertToByteArray(kafkaKeyStr: String) : Array[Byte] = {
  val bytesInStr = kafkaKeyStr.slice(1, kafkaKeyStr.length-1).split(",")
  val byteArray = bytesInStr.map(_.toByte)
  byteArray
}

  val connStr = "YOUR_CONNECTION_STRING"
  val eventhubParameters = EventHubsConf(connStr) // YOUR_SETTING
  val incomingStream = spark.readStream.format("eventhubs").options(eventhubParameters.toMap).load()

  val receivedKafkaBodyAndKey = 
        incomingStream
           .withColumn("Body", $"body".cast(StringType))
           .withColumn("kafkaKey", $"systemProperties".getItem("x-opt-kafka-key").cast(StringType)) 
           .select("Body", "kafkaKey")
  
  val kafkaBodyAndKey = 
        receivedKafkaBodyAndKey
            .map(r => {
                  val bodyStr = r(0).asInstanceOf[String]
                  val kafkaKeyBytesInString = r(1).asInstanceOf[String]
                  val keyInByteArray = convertToByteArray(kafkaKeyBytesInString)
                  val kafkaKeyInLong = ByteBuffer.wrap(keyInByteArray).getLong
                  (bodyStr, kafkaKeyInLong)})
            .withColumnRenamed("_1","msgBody")
            .withColumnRenamed("_2","msgKey")
            
  kafkaBodyAndKey.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
```