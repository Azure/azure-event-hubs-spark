package org.apache.spark.eventhubs.client

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
