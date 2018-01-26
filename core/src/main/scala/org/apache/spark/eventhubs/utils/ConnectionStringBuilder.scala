/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.eventhubs.utils

import java.net.{ URI, URISyntaxException }
import java.time.Duration
import java.time.format.DateTimeParseException
import java.util.Locale
import java.util.regex.Pattern

import com.microsoft.azure.eventhubs.{ MessagingFactory, StringUtil }

/**
 * [[ConnectionStringBuilder]] can be used to construct a connection string which can establish communication with EventHub instances.
 * In addition to constructing a connection string, the [[ConnectionStringBuilder]] can be used to modify an existing connection string.
 * Using the [[ConnectionStringBuilder]] is NOT required. The simplest way to get a connection string is to simply copy it from the Azure portal.
 * <p> Sample Code:
 * <pre>
 *  {{{
 *  // Construct a new connection string
 *  val connStr = ConnectionStringBuilder()
 *    .setNamespaceName("EventHubsNamespaceName")
 * 	  .setEventHubName("EventHubsEntityName")
 * 	  .setSasKeyName("SharedAccessSignatureKeyName")
 * 	  .setSasKey("SharedAccessSignatureKey")
 *    .build()
 *
 *  // Modify an existing connection string
 *  val connStr = ConnectionStringBuilder(existingConnectionString)
 *    .setEventHubName("SomeOtherEventHubsName")
 *    .setOperationTimeout(Duration.ofSeconds(30)
 *    .build()
 *  }}}
 * </pre>
 * <p>
 *
 * Creates an empty [[ConnectionStringBuilder]]. At minimum, a namespace name, an entity path, SAS key name, and SAS key
 * need to be set before a valid connection string can be built.
 *
 * For advanced users, the following replacements can be done:
 * <ul>
 * <li>An endpoint can be provided instead of a namespace name.</li>
 * <li>A SAS token can be provided instead of a SAS key name and SAS key.</li>
 * <li>Optionally, users can set an operation timeout instead of using the default value.</li>
 * </ul>
 */
class ConnectionStringBuilder private () {

  import ConnectionStringBuilder._

  /**
   * @param connectionString EventHubs ConnectionString
   * @throws IllegalConnectionStringFormatException when the format of the ConnectionString is not valid
   */
  private def this(connectionString: String) {
    this()
    parseConnectionString(connectionString)
  }

  private var endpoint: URI = _
  private var eventHubName: String = _
  private var sharedAccessKeyName: String = _
  private var sharedAccessKey: String = _
  private var sharedAccessSignature: String = _
  private var operationTimeout: Duration = _

  /**
   * Get the endpoint which can be used to connect to the EventHub instance.
   *
   * @return The currently set endpoint
   */
  def getEndpoint: URI = {
    this.endpoint
  }

  /**
   * Set an endpoint which can be used to connect to the EventHub instance.
   *
   * @param endpoint is a combination of the namespace name and domain name. Together, these pieces make a valid
   *                 endpoint. For example, the default domain name is "servicebus.windows.net", so a sample endpoint
   *                 would look like this: "sb://namespace_name.servicebus.windows.net".
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setEndpoint(endpoint: URI): ConnectionStringBuilder = {
    this.endpoint = endpoint
    this
  }

  /**
   * Set an endpoint which can be used to connect to the EventHub instance.
   *
   * @param namespaceName     the name of the namespace to connect to.
   * @param domainName        identifies the domain the namespace is located in. For non-public and national clouds,
   *                          the domain will not be "servicebus.windows.net". Available options include:
   *                          - "servicebus.usgovcloudapi.net"
   *                          - "servicebus.cloudapi.de"
   *                          - "servicebus.chinacloudapi.cn"
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setEndpoint(namespaceName: String, domainName: String): ConnectionStringBuilder = {
    try {
      this.endpoint = new URI(s"$DefaultProtocol$namespaceName.$domainName")
    } catch {
      case exception: URISyntaxException =>
        throw new IllegalConnectionStringFormatException(
          String.format(Locale.US, "Invalid namespace name: %s", namespaceName),
          exception)
    }
    this
  }

  /**
   * Set a namespace name which will be used to connect to an EventHubs instance. This method adds
   * "servicebus.windows.net" as the default domain name.
   *
   * @param namespaceName the name of the namespace to connect to.
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setNamespaceName(namespaceName: String): ConnectionStringBuilder = {
    this.setEndpoint(namespaceName, DefaultDomainName)
  }

  /**
   * Get the entity path value from the connection string.
   *
   * @return Entity Path
   */
  def getEventHubName: String = {
    this.eventHubName
  }

  /**
   * Set the entity path value from the connection string.
   *
   * @param eventHubName the name of the Event Hub to connect to.
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setEventHubName(eventHubName: String): ConnectionStringBuilder = {
    this.eventHubName = eventHubName
    this
  }

  /**
   * Get the shared access policy key value from the connection string
   *
   * @return Shared Access Signature key
   */
  def getSasKey: String = {
    this.sharedAccessKey
  }

  /**
   * Set the shared access policy key value from the connection string
   *
   * @param sasKey the SAS key
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setSasKey(sasKey: String): ConnectionStringBuilder = {
    this.sharedAccessKey = sasKey
    this
  }

  /**
   * Get the shared access policy owner name from the connection string
   *
   * @return Shared Access Signature key name.
   */
  def getSasKeyName: String = {
    this.sharedAccessKeyName
  }

  /**
   * Set the shared access policy owner name from the connection string
   *
   * @param sasKeyName the SAS key name
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setSasKeyName(sasKeyName: String): ConnectionStringBuilder = {
    this.sharedAccessKeyName = sasKeyName
    this
  }

  /**
   * Get the shared access signature (also referred as SAS Token) from the connection string
   *
   * @return Shared Access Signature
   */
  def getSharedAccessSignature: String = {
    this.sharedAccessSignature
  }

  /**
   * Set the shared access signature (also referred as SAS Token) from the connection string
   *
   * @param sharedAccessSignature the shared access key signature
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setSharedAccessSignature(sharedAccessSignature: String): ConnectionStringBuilder = {
    this.sharedAccessSignature = sharedAccessSignature
    this
  }

  /**
   * OperationTimeout is applied in erroneous situations to notify the caller about the relevant [[com.microsoft.azure.eventhubs.EventHubException]]
   *
   * @return operationTimeout
   */
  def getOperationTimeout: Duration = {
    if (this.operationTimeout == null) {
      MessagingFactory.DefaultOperationTimeout
    } else {
      this.operationTimeout
    }
  }

  /**
   * Set the OperationTimeout value in the Connection String. This value will be used by all operations which uses this [[ConnectionStringBuilder]], unless explicitly over-ridden.
   * <p>ConnectionString with operationTimeout is not inter-operable between java and clients in other platforms.
   *
   * @param operationTimeout Operation Timeout
   * @return the { @link ConnectionStringBuilder} being set.
   */
  def setOperationTimeout(operationTimeout: Duration): ConnectionStringBuilder = {
    this.operationTimeout = operationTimeout
    this
  }

  /**
   * Identical to [[build]].
   */
  override def toString: String = build

  /**
   * Returns an inter-operable connection string that can be used to connect to EventHubs instances.
   *
   * @return connection string
   */
  def build: String = {
    val connStrBuilder: StringBuilder = new StringBuilder

    if (this.endpoint != null) {
      connStrBuilder.append(
        s"$EndpointConfigName$KeyValueSeparator${this.endpoint.toString}$KeyValuePairDelimiter")
    }
    if (!StringUtil.isNullOrWhiteSpace(this.eventHubName)) {
      connStrBuilder.append(
        s"$EntityPathConfigName$KeyValueSeparator${this.eventHubName}$KeyValuePairDelimiter"
      )
    }
    if (!StringUtil.isNullOrWhiteSpace(this.sharedAccessKeyName)) {
      connStrBuilder.append(
        s"$SharedAccessKeyNameConfigName$KeyValueSeparator${this.sharedAccessKeyName}$KeyValuePairDelimiter"
      )
    }
    if (!StringUtil.isNullOrWhiteSpace(this.sharedAccessKey)) {
      connStrBuilder.append(
        s"$SharedAccessKeyConfigName$KeyValueSeparator${this.sharedAccessKey}$KeyValuePairDelimiter"
      )
    }
    if (!StringUtil.isNullOrWhiteSpace(this.sharedAccessSignature)) {
      connStrBuilder.append(
        s"$SharedAccessSignatureConfigName$KeyValueSeparator${this.sharedAccessSignature}$KeyValuePairDelimiter"
      )
    }
    if (this.operationTimeout != null) {
      connStrBuilder.append(
        s"$OperationTimeoutConfigName$KeyValueSeparator${this.operationTimeout}$KeyValuePairDelimiter"
      )
    }
    connStrBuilder.deleteCharAt(connStrBuilder.length - 1)
    connStrBuilder.toString
  }

  private def parseConnectionString(connectionString: String): Unit = {
    if (StringUtil.isNullOrWhiteSpace(connectionString)) {
      throw new IllegalConnectionStringFormatException("connectionString cannot be empty")
    }

    val connection = KeyValuePairDelimiter + connectionString
    val keyValuePattern = Pattern.compile(KeysWithDelimitersRegex, Pattern.CASE_INSENSITIVE)
    val values = keyValuePattern.split(connection)
    val keys = keyValuePattern.matcher(connection)

    if (values == null || values.length <= 1 || keys.groupCount == 0) {
      throw new IllegalConnectionStringFormatException("Connection String cannot be parsed.")
    }

    if (!StringUtil.isNullOrWhiteSpace(values(0))) {
      throw new IllegalConnectionStringFormatException(
        String.format(Locale.US, "Cannot parse part of ConnectionString: %s", values(0)))
    }

    var valueIndex: Int = 0
    while (keys.find) {
      valueIndex += 1
      var key = keys.group
      key = key.substring(1, key.length - 1)

      if (values.length < valueIndex + 1) {
        throw new IllegalConnectionStringFormatException(
          s"Value for the connection string parameter name: $key, not found")
      }

      if (key.equalsIgnoreCase(EndpointConfigName)) {
        if (this.endpoint != null) {
          // we have parsed the endpoint once, which means we have multiple config which is not allowed
          throw new IllegalConnectionStringFormatException(
            s"Multiple $EndpointConfigName and/or $HostnameConfigName detected. Make sure only one is defined"
          )
        }

        try {
          this.endpoint = new URI(values(valueIndex))
        } catch {
          case e: URISyntaxException =>
            throw new IllegalConnectionStringFormatException(
              s"$EndpointConfigName should be in format sb://<namespaceName>.<domainName>",
              e)
        }
      } else if (key.equalsIgnoreCase(HostnameConfigName)) {
        if (this.endpoint != null) {
          throw new IllegalConnectionStringFormatException(
            s"Multiple $EndpointConfigName and/or $HostnameConfigName detected. Make sure only one is defined"
          )
        }

        try {
          this.endpoint = new URI(s"$DefaultProtocol${values(valueIndex)}")
        } catch {
          case e: URISyntaxException =>
            throw new IllegalConnectionStringFormatException(
              s"$HostnameConfigName should be a fully quantified host name address",
              e)
        }
      } else if (key.equalsIgnoreCase(SharedAccessKeyNameConfigName)) {
        this.sharedAccessKeyName = values(valueIndex)
      } else if (key.equalsIgnoreCase(SharedAccessKeyConfigName)) {
        this.sharedAccessKey = values(valueIndex)
      } else if (key.equalsIgnoreCase(SharedAccessSignatureConfigName)) {
        this.sharedAccessSignature = values(valueIndex)
      } else if (key.equalsIgnoreCase(EntityPathConfigName)) {
        this.eventHubName = values(valueIndex)
      } else if (key.equalsIgnoreCase(OperationTimeoutConfigName)) {
        try {
          this.operationTimeout = Duration.parse(values(valueIndex))
        } catch {
          case e: DateTimeParseException =>
            throw new IllegalConnectionStringFormatException(
              "Invalid value specified for property 'Duration' in the ConnectionString.",
              e)
        }
      } else {
        throw new IllegalConnectionStringFormatException(
          s"Illegal connection string parameter name: $key")
      }
    }
  }

  class IllegalConnectionStringFormatException(private val msg: String = "",
                                               private val cause: Throwable = None.orNull)
      extends IllegalArgumentException(msg, cause)

}

object ConnectionStringBuilder {
  private val DefaultProtocol = "sb://"
  private val DefaultDomainName = "servicebus.windows.net"

  private val HostnameConfigName = "Hostname" // Hostname is a key that is used in IoTHub.
  private val EndpointConfigName = "Endpoint" // Endpoint key is used in EventHubs. It's identical to Hostname in IoTHub.
  private val EntityPathConfigName = "EntityPath"
  private val OperationTimeoutConfigName = "OperationTimeout"
  private val KeyValueSeparator = "="
  private val KeyValuePairDelimiter = ";"
  private val SharedAccessKeyNameConfigName = "SharedAccessKeyName" // We use a (KeyName, Key) pair OR the SAS token - never both.
  private val SharedAccessKeyConfigName = "SharedAccessKey"
  private val SharedAccessSignatureConfigName = "SharedAccessSignature"

  private val AllKeyEnumerateRegex = "(" + HostnameConfigName + "|" + EndpointConfigName + "|" +
    SharedAccessKeyNameConfigName + "|" + SharedAccessKeyConfigName + "|" + SharedAccessSignatureConfigName +
    "|" + EntityPathConfigName + "|" + OperationTimeoutConfigName + "|" + ")"
  private val KeysWithDelimitersRegex = KeyValuePairDelimiter + AllKeyEnumerateRegex + KeyValueSeparator

  /**
   * @return an empty [[ConnectionStringBuilder]]
   */
  def apply(): ConnectionStringBuilder = new ConnectionStringBuilder

  /**
   * @param connectionString is an existing connection string. [[ConnectionStringBuilder]] will parse this argument and allow
   *                         values to be modified with setter methods.
   * @return a [[ConnectionStringBuilder]] with data from the previous connection string
   */
  def apply(connectionString: String): ConnectionStringBuilder = {
    new ConnectionStringBuilder(connectionString)
  }
}
