# Use AAD Authentication to Connect Eventhubs Guide for PySpark

This guide will show you how you can 
<a href="https://docs.microsoft.com/en-us/azure/event-hubs/authenticate-application" target="_blank">use AAD authentication to access Eventhubs</a> in Python applications.

Similar to steps discussed in the [Use AAD Authentication to Connect Eventhubs](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/use-aad-authentication-to-connect-eventhubs.md)
document, first you need to create a callback class extends from `org.apache.spark.eventhubs.utils.AadAuthenticationCallback`.
Please note that since the connector is using reflection to instantiate the callback class on each executor node, the callback class definition should be packaged in a jar file and be added to your cluster.

### Callback Class Example

Below is an example of a Callback class implementation that extends the `org.apache.spark.eventhubs.utils.AadAuthenticationCallback` and contains the secrets for authentication.
You can package this class in a Jar file and add the Jar file to your cluster.

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

### Use Callback Class in Python Applications

Once you have the Callback class in the cluster, you can set Event Hubs configuration dictionary to use Azure Active Directory Authentication:

```python
ehConf = {}

# Set Aad authentication connection string
myURI = sc._jvm.java.net.URI("your-ehs-endpoint")
connectionString = sc._jvm.org.apache.spark.eventhubs.ConnectionStringBuilder().setAadAuthConnectionString(myURI, "your-ehs-name").build()
# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Set 'useAadAuth' to True and 'aadAuthCallback' to the callback class name
authCallback = sc._jvm.AuthBySecretCallBack()
ehConf["eventhubs.useAadAuth"] = True
ehConf["eventhubs.aadAuthCallback"] = authCallback.getClass().getName()

# For more ehConf options please refer to the 'structured-streaming-pyspark.md' document
# Create a stream using the config dictionary
df = spark.readStream.format("eventhubs").options(**ehConf).load()
ds = df.select("body").writeStream.format("console").start()
```

