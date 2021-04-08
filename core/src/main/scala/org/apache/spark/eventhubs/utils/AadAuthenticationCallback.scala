package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider.AuthenticationCallback

abstract class AadAuthenticationCallback(val params: String*) extends AuthenticationCallback with Serializable {
  def authority: String
}
