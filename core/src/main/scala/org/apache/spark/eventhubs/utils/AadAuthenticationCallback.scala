package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider.AuthenticationCallback

abstract class AadAuthenticationCallback(val args: String*) extends AuthenticationCallback with Serializable {
  def authority: String
}
