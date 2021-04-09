# Use AAD Authentication to Connect Eventhubs
This guide will show you how you can 
<a href="https://docs.microsoft.com/en-us/azure/event-hubs/authenticate-application" target="_blank">use AAD authentication to access Eventhubs</a>.

* [Use Service Principal + Secret to authorize](#use-service-principal-+-secret-to-authorize)
* [Use Service Principal + Certificate to authorize](#use-service-principal-+-certificate-to-authorize)


## Use Service Principal + Secret to authorize
First, you need to create a callback class extends from `org.apache.spark.eventhubs.utils.AadAuthenticationCallback`,
```scala
import java.util.Collections
import java.util.concurrent.CompletableFuture

import com.microsoft.aad.msal4j.{IAuthenticationResult, _}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

class AuthBySecretCallBack(params: String*) extends AadAuthenticationCallback(params) {

  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
  override def authority: String = "your-tenant-id" //or taken from "params"

  val clientId: String = "your-client-id" //or taken from "params"
  val clientSecret: String = "your-client-secret" //or taken from "params"

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = try {
    var app = ConfidentialClientApplication
      .builder("clientId", ClientCredentialFactory.createFromSecret(this.clientSecret))
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
and then set the authentication to use AAD auth.
```scala
val connectionString = ConnectionStringBuilder()
  .setAadAuthConnectionString(new URI("your-ehs-endpoint"), "your-ehs-name")
  .build
val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup("consumerGroup")
  .setAadAuthCallback(new AuthBySecretCallBack(/* optional "params" can be passed here */))
```


## Use Service Principal + Certificate to authorize

Alternatively, you can use certificate to make your connections.

```scala
import java.io.{ByteArrayInputStream, File}
import java.util.Collections
import java.util.concurrent.CompletableFuture

import com.microsoft.aad.msal4j.{ClientCredentialFactory, ClientCredentialParameters, ConfidentialClientApplication, IAuthenticationResult}
import org.apache.commons.io.FileUtils
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback
class AuthByCertCallBack(params: String*) extends AadAuthenticationCallback(params) {
  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
  val clientId: String = "your-client-id" //or taken from "params"
  val cert: Array[Byte] = FileUtils.readFileToByteArray(new File("your-cert-local-path")) //or taken from "params"
  val certPassword: String = "password-of-your-cert" //or taken from "params"

  override def authority: String = "your-tenant-id" //or taken from "params"
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
  .setAadAuthCallback(new AuthByCertCallBack(/* optional "params" can be passed here */))
```
