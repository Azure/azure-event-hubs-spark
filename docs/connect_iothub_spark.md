# Connect Iot Hub and Spark with the Spark-EventHubs Connector

### From your IoT Hub instance, get the ```SasKeyName```, ```SasKey```, ```EventHub-compatible name```, and ```EventHub-compatible Namespace name```. 
   - ```SasKeyName``` and ```SasKey``` 
     1. Go to the [Azure Portal](www.ms.portal.azure.com) and find your IoT Hub instance
     2. Click on **Shared access policies** under **Settings**
     3. Click on **iothubowner**
     4. In the blade that opens up, you'll see your **Primary key**. That is your ```SasKey```. 
     5. **iothubowner** is your ```SasKeyName```.
    
   - ```EventHub-compatible name``` and ```EventHub-compatible Namepsace name```
     1. Still on your IoT Hub instance within the [Azure Portal](www.ms.portal.azure.com), click on **Endpoints** under **Messaging**
     2. Click on **Events**
     3. In the blade that opens up, you'll see your ```EventHub-compatible name``` and ```EventHub-compatible endpoint```.
     4. Your ```EventHub-compatible endpoint``` will have the following form: 
        <br>```Endpoint=sb://iothub-xxxxxxxxxx.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=xxxxxxxxxx```
        <br> The part that reads ```iothub-xxxxxxxxxx``` is your ```EventHub-compatible Namespace name```. 
        
### Setup your ```eventhubParameters```
 - Using the info we found from the last section, we need to create our ```eventhubParamters``` which is used to create DStreams with the Spark-EventHubs connector. It should look something like:
    ```scala
    val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> "iothubowner",							
      "eventhubs.policykey" -> <SasKey>,						
      "eventhubs.namespace" -> <EventHub-compatible Namespace name>,	
      "eventhubs.name" -> <EventHub-compatible name>,					
      "eventhubs.partition.count" -> "4",			// 4 is the default value! 
      "eventhubs.consumergroup" -> "$Default",		// Check your partition count and consumer groups in the same place you found the EventHub name.
    )
    ```
   
### You're All Set!
At this point, you'll be able to connect to your IoT Hub! See our [README](../README.md) or [Getting Started](getting_started.md) page for more info about setting up the Spark-EventHubs connector.
