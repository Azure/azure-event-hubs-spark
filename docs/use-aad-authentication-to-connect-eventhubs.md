# Use AAD Authentication to Connect Eventhubs
This guide will show you how you can 
<a href="https://docs.microsoft.com/en-us/azure/event-hubs/authenticate-application" target="_blank">use AAD authentication to access Eventhubs</a>.

* [Use Service Principal with Secret to Authorize](#use-service-principal-with-secret-to-authorize)
  * [Write Secrets in Callback Class](#write-secret-in-callback-class)
  * [Pass Secrets to Callback Class](#pass-secret-to-callback-class)
* [Use Service Principal with Certificate to Authorize](#use-service-principal-with-certificate-to-authorize)


## Use Service Principal with Secret to Authorize
First, you need to create a callback class extends from `org.apache.spark.eventhubs.utils.AadAuthenticationCallback`. There are two options on how the callback class can access the secrets. Either set the secrets directly in the class definition, or pass teh secrets as a sequence of strings parameter to the callback class.

### Write Secret in Callback Class
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
Another option is to pass the secrets as a sequence of strings to the callback class. For instance, if you want to read the secrets from a [secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes), you can use `dbutils` API to get the secrets on the driver and pass those to the callback class. 
Here is an example showing how you can do so:
```scala
import java.util.Collections
import java.util.concurrent.CompletableFuture
import com.microsoft.aad.msal4j.{IAuthenticationResult, _}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

class AuthBySecretCallBackWithParams(params: Seq[String]) extends AadAuthenticationCallback{

  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
  
  override def authority: String = params(0)
  val clientId: String = params(1)
  val clientSecret: String = params(2)

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
val params: Seq[String] = Seq(dbutils.secrets.get(scope = "your_secret_scope", key = "your_tenantid_key"),
                              dbutils.secrets.get(scope = "your_secret_scope", key = "your_clientid_key"),
                              dbutils.secrets.get(scope = "your_secret_scope", key = "your_clientsecret_key"))

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
