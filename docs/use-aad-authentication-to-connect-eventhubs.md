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

case class AuthBySecretCallBack(clientId: String, clientSecret: String) extends AadAuthenticationCallback{

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = try {
    var app = ConfidentialClientApplication
      .builder(clientId, new ClientSecret(this.clientSecret))
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
val connectionString = "Endpoint=sb://SAMPLE;EntityPath=EVENTHUBS_NAME"
val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup("consumerGroup")
  .setUseAadAuth(true)
  .setAadAuthTenantId("tenant_guid")
  .setAadAuthCallback(AuthBySecretCallBack())
```


## Use Service Principal + Certificate to authorize

Alternatively, you can use certificate to make your connections.

```scala
import java.io.ByteArrayInputStream
import java.util.Collections
import java.util.concurrent.CompletableFuture

import com.microsoft.aad.msal4j.{ClientCredentialFactory, ClientCredentialParameters, ConfidentialClientApplication, IAuthenticationResult}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

case class AuthByCertCallBack(clientId: String, cert: Array[Byte], certPassword: String)
    extends AadAuthenticationCallback {
  override def acquireToken(audience: String,
                            authority: String,
                            state: Any): CompletableFuture[String] =
    try {
      val app = ConfidentialClientApplication
        .builder(clientId,
                 ClientCredentialFactory.create(new ByteArrayInputStream(cert), certPassword))
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
val connectionString = "Endpoint=sb://SAMPLE;EntityPath=EVENTHUBS_NAME"
val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup("consumerGroup")
  .setUseAadAuth(true)
  .setAadAuthTenantId("tenant_guid")
  .setAadAuthCallback(AuthByCertCallBack())
```
