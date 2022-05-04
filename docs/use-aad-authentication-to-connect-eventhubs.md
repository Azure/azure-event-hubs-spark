# Use AAD Authentication to Connect Event Hubs
This guide will show you how you can 
<a href="https://docs.microsoft.com/en-us/azure/event-hubs/authenticate-application" target="_blank">use AAD authentication to access Event Hubs</a>.

* [Use Service Principal with Secret to Authorize](#use-service-principal-with-secret-to-authorize)
  * [Write Secrets in Callback Class](#write-secrets-in-callback-class)
  * [Pass Secrets to Callback Class](#pass-secrets-to-callback-class)
* [Use Service Principal with Certificate to Authorize](#use-service-principal-with-certificate-to-authorize)


## Use Service Principal with Secret to Authorize
First, you need to create a callback class extends from `org.apache.spark.eventhubs.utils.AadAuthenticationCallback`. There are two options on how the callback class can access the secrets. Either set the secrets directly in the class definition, or pass the secrets in a properties bag of type `Map[String, Object]` to the callback class.
Please note that since the connector is using reflection to instantiate the callback class on each executor node, the callback class definition should be packaged in a jar file and be added to your cluster.

### Write Secrets in Callback Class
In this case, you set the required secrets in the callback class as shown in the below example:

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

Now you can use the `setAadAuthCallback` option in `EventHubsConf` to us AAD authentication to connect to your EventHub instance.

```scala
val connectionString = ConnectionStringBuilder()
  .setAadAuthConnectionString(new URI("your-ehs-endpoint"), "your-ehs-name")
  .build
  
val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup("consumerGroup")
  .setAadAuthCallback(AuthBySecretCallBack())
```


### Pass Secrets to Callback Class
Another option is to pass the secrets in a properties bag to the callback class. For instance, if you want to read the secrets from a [secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes), you can use `dbutils` API to get the secrets on the driver and pass those to the callback class. Note that the callback class only accepts one parameter of type `Map[String, Object]`.
Here is an example showing how you can do so:

```scala
import java.util.Collections
import java.util.concurrent.CompletableFuture
import com.microsoft.aad.msal4j.{IAuthenticationResult, _}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

class AuthBySecretCallBackWithParams(params: Map[String, Object]) extends AadAuthenticationCallback{

  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
  
  override def authority: String = params("authority").asInstanceOf[String]
  val clientId: String = params("clientId").asInstanceOf[String]
  val clientSecret: String = params("clientSecret").asInstanceOf[String]

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

In this case you should use both `setAadAuthCallback` and `setAadAuthCallbackParams` options in `EventHubsConf` to us AAD authentication to connect to your EventHub instance.

```scala
val params: Map[String, String] = Map("authority" -> dbutils.secrets.get(scope = "nykvsecrets", key = "ehaadtesttenantid"),
									  "clientId" -> dbutils.secrets.get(scope = "nykvsecrets", key = "ehaadtestclientid"),
									  "clientSecret" -> dbutils.secrets.get(scope = "nykvsecrets", key = "ehaadtestclientsecret"))

val connectionString = ConnectionStringBuilder()
  .setAadAuthConnectionString(new URI("your-ehs-endpoint"), "your-ehs-name")
  .build

val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup("consumerGroup")
  .setAadAuthCallback(new AuthBySecretCallBackWithParams(params))
  .setAadAuthCallbackParams(params)
```


## Use Service Principal with Certificate to Authorize

Alternatively, you can use certificate to make your connections.

```scala
import java.io.{ByteArrayInputStream, File}
import java.util.Collections
import java.util.concurrent.CompletableFuture

import com.microsoft.aad.msal4j.{ClientCredentialFactory, ClientCredentialParameters, ConfidentialClientApplication, IAuthenticationResult}
import org.apache.commons.io.FileUtils
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback
case class AuthByCertCallBack() extends AadAuthenticationCallback {
  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
  val clientId: String = "your-client-id"
  val cert: Array[Byte] = FileUtils.readFileToByteArray(new File("your-cert-local-path"))
  val certPassword: String = "password-of-your-cert"

  override def authority: String = "your-tenant-id"
  override def acquireToken(audience: String,
                            authority: String,
                            state: Any): CompletableFuture[String] =
    try {
      val app = ConfidentialClientApplication
        .builder(clientId,
          ClientCredentialFactory.createFromCertificate(new ByteArrayInputStream(cert), certPassword))
        .authority("https://login.microsoftonline.com/" + authority)
        .build
      val parameters =
        ClientCredentialParameters.builder(Collections.singleton(audience + ".default")).build
      app
        .acquireToken(parameters)
        .thenApply((result: IAuthenticationResult) => result.accessToken())
    } catch {
      case e: Exception =>
        val failed = new CompletableFuture[String]
        failed.completeExceptionally(e)
        failed
    }
}
```
and then set the authentication to use AAD auth.
```scala
val connectionString = ConnectionStringBuilder()
  .setAadAuthConnectionString(new URI("your-ehs-endpoint"), "your-ehs-name")
  .build
val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup("consumerGroup")
  .setAadAuthCallback(AuthByCertCallBack())
```
