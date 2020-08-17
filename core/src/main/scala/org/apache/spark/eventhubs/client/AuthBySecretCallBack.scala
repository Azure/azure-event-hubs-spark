package org.apache.spark.eventhubs.client

import java.io.InputStream
import java.util.Collections
import java.util.concurrent.CompletableFuture

import com.microsoft.aad.msal4j.{IAuthenticationResult, _}
import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider

case class AuthBySecretCallBack(clientId: String, clientSecret: String) extends AzureActiveDirectoryTokenProvider.AuthenticationCallback {

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = try {
    var app = ConfidentialClientApplication
      .builder(clientId, new ClientSecret(this.clientSecret))
      .authority(authority)
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

