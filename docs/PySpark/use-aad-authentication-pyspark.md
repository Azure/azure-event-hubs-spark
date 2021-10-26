# Use AAD Authentication to Connect Eventhubs Guide for PySpark

This guide will show you how you can 
<a href="https://docs.microsoft.com/en-us/azure/event-hubs/authenticate-application" target="_blank">use AAD authentication to access Eventhubs</a> in Python applications.

Similar to steps discussed in the [Use AAD Authentication to Connect Eventhubs](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/use-aad-authentication-to-connect-eventhubs.md)
document, first you need to create a callback class extends from `org.apache.spark.eventhubs.utils.AadAuthenticationCallback`.


## Use Service Principal with Secret to Authorize
First, you need to create a callback class extends from `org.apache.spark.eventhubs.utils.AadAuthenticationCallback`. 
There are two options on how the callback class can access the secrets. Either set the secrets directly in the class 
definition, or pass the secrets in a properties bag to the callback class. Please note that since the connector is 
using reflection to instantiate the callback class on each executor node, the callback class definition should be 
packaged in a jar file and be added to your cluster.


### Write Secrets in Callback Class

Below is an example of a Callback class implementation that extends the `org.apache.spark.eventhubs.utils.AadAuthenticationCallback` 
and contains the secrets for authentication. You can package this class in a Jar file and add the Jar file to your cluster.

```scala
import java.util.Collections
import java.util.concurrent.CompletableFuture
import com.microsoft.aad.msal4j.{IAuthenticationResult, _}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

case class AuthBySecretCallBack() extends AadAuthenticationCallback{

  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
  
  override def authority: String = "your-tenant-id"
  val clientId: String = "your-client-id"
  val clientSecret: String = "your-client-secret"

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = try {
    var app = ConfidentialClientApplication
      .builder(clientId, ClientCredentialFactory.createFromSecret(this.clientSecret))
      .authority("https://login.microsoftonline.com/" + authority)
      .build

    val parameters = ClientCredentialParameters.builder(Collections.singleton(audience + ".default")).build

    app.acquireToken(parameters).thenApply((result: IAuthenticationResult) => result.accessToken())
  } catch {
    case e: Exception =>
      val failed = new CompletableFuture[String]
      failed.completeExceptionally(e)
      failed
  }
}
```

Once you have the Callback class in the cluster, you can set Event Hubs configuration dictionary to use Azure Active 
Directory Authentication. Please note that in PySpark applications you have to set the `eventhubs.useAadAuth` flag to
`True` in addition to setting the callback class name.

```python
ehConf = {}

# Set Aad authentication connection string
myURI = sc._jvm.java.net.URI("your-ehs-endpoint")
connectionString = sc._jvm.org.apache.spark.eventhubs.ConnectionStringBuilder().setAadAuthConnectionString(myURI, "your-ehs-name").build()
# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Set 'useAadAuth' to True and 'aadAuthCallback' to the callback class name
authCallback = sc._jvm.AuthBySecretCallBack()
ehConf['eventhubs.useAadAuth'] = True
ehConf['eventhubs.aadAuthCallback'] = authCallback.getClass().getName()

# For more ehConf options please refer to the 'structured-streaming-pyspark.md' document
# Create a stream using the config dictionary
df = spark.readStream.format("eventhubs").options(**ehConf).load()
ds = df.select("body").writeStream.format("console").start()
```


### Pass Secrets to Callback Class

Another option is to pass the secrets in a properties bag to the callback class. You need to implement the callback 
class extending the `org.apache.spark.eventhubs.utils.AadAuthenticationCallback` trait and load it to your cluster.

```scala
import java.util.Collections
import java.util.concurrent.CompletableFuture
import com.microsoft.aad.msal4j.{IAuthenticationResult, _}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

class AuthBySecretCallBackWithParamsPySpark(params: scala.collection.immutable.Map[String, Object]) extends AadAuthenticationCallback {
  
  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
 
   override def authority: String = params.get("authority") match {
	  case None => ""
	  case Some(obj) => obj.asInstanceOf[String]
   }
   val clientId: String = params.get("clientId") match {
	  case None => ""
	  case Some(obj) => obj.asInstanceOf[String]
   }
   val clientSecret: String = params.get("clientSecret") match {
	  case None => ""
	  case Some(obj) => obj.asInstanceOf[String]
   }

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = try {
    var app = ConfidentialClientApplication
      .builder(clientId, ClientCredentialFactory.createFromSecret(this.clientSecret))
      .authority("https://login.microsoftonline.com/" + authority)
      .build

    val parameters = ClientCredentialParameters.builder(Collections.singleton(audience + ".default")).build

    app.acquireToken(parameters).thenApply((result: IAuthenticationResult) => result.accessToken())
  } catch {
    case e: Exception =>
      val failed = new CompletableFuture[String]
      failed.completeExceptionally(e)
      failed
  }
}
```

Now you should set all `eventhubs.useAadAuth`, `eventhubs.aadAuthCallback` and `eventhubs.AadAuthCallbackParams` 
options in `EventHubsConf` to us AAD authentication to connect to your EventHub instance. 

```python
ehConf = {}

# Set Aad authentication connection string
myURI = sc._jvm.java.net.URI("your-ehs-endpoint")
connectionString = sc._jvm.org.apache.spark.eventhubs.ConnectionStringBuilder().setAadAuthConnectionString(myURI, "your-ehs-name").build()
# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Set 'useAadAuth' to True, 'aadAuthCallback' to the callback class name, and 'AadAuthCallbackParams' to the dictionary containg the secrets
params = {}
params["authority"] = "your-tenant-id"
params["clientId"] = "your-client-id"
params["clientSecret"] = "your-client-secret"

ehConf['eventhubs.useAadAuth'] = True
ehConf['eventhubs.aadAuthCallback'] = "AuthBySecretCallBackWithParamsPySpark"
ehConf['eventhubs.AadAuthCallbackParams'] = json.dumps(params)

# For more ehConf options please refer to the 'structured-streaming-pyspark.md' document
# Create a stream using the config dictionary
df = spark.readStream.format("eventhubs").options(**ehConf).load()
ds = df.select("body").writeStream.format("console").start()
```
