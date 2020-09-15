package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider.AuthenticationCallback

trait AadAuthenticationCallback extends AuthenticationCallback with Serializable {

}
