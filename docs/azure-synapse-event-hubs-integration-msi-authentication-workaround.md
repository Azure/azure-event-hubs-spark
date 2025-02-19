# Azure Synapse Event Hubs Integration: MSI Authentication Workaround

## Objective:
This guide provides a workaround for integrating Azure Event Hubs with Synapse, addressing the limitation where Synapse does not currently support Event Hubs Managed Identity (MSI). It outlines the steps to modify Event Hubs Spark, rebuild it as a private library, and upload it to Synapse for seamless integration.

---

## Frequently Asked Questions (FAQ)

1. **Does Event Hubs support Managed Identity (MSI)?**
   - Yes, Event Hubs supports Managed Identity (MSI).

2. **Does Event Hubs support Managed System Identity in Synapse?**
   - No, as of today (5/16/24), Synapse does not extend support for Event Hubs MSI. The Synapse team is planning to address this issue in the future.

3. **Does Event Hubs support Managed System Identity in Azure Databricks?**
   - No, as of today (6/18/24), Azure Databricks does not support Event Hubs MSI.

---

## MSI Workaround Steps

Some users may encounter a **ClassNotFoundException** when using the custom callback function, caused by an issue with the Spark library. To resolve this issue, follow these steps:

### Step 1: Modify the EventHub Spark Connector

1. **Review the Required Pull Request Commit**:
   - For **Spark 2.4** or versions below Spark 3.0, refer to the necessary changes in this [GitHub pull request](https://github.com/Azure/azure-event-hubs-spark/pull/685).
   - For **Spark 3.0+**, use the `classForName(String className, boolean initialize, boolean noSparkClassLoader)` method to dynamically load classes.

2. **Modify the Connector**:
   - Download the changes from the pull request or manually implement the necessary code updates to handle class loading and compatibility with the Spark version you are using.

---

### Step 2: Rebuild the Spark Connector

1. **Clone the Repository**:
   - Clone the Azure Event Hubs Spark connector repository to your local machine.

2. **Apply the Modifications**:
   - Apply the changes from the commit or manually incorporate the required code fixes.

3. **Rebuild the Connector**:
   - Use Maven or SBT to rebuild the connector:
     ```bash
     mvn clean package
     ```

4. **Build the JAR Files**:
   - Ensure the rebuild generates the necessary JAR files for Synapse.

---

### Step 3: Add the Modified Connector as a Private Library in Synapse

1. **Access Synapse Studio**:
   - Open Synapse Studio in the Azure portal from your Synapse workspace.

2. **Upload the Custom JAR**:
   - In the Synapse Studio's **Manage** section, navigate to **Linked Services**, then click **Libraries** and choose **Upload**.
   - Upload the JAR file generated in the previous step.

3. **Add the Library to a Spark Pool**:
   - After uploading, associate the custom library with your Spark pool. This allows your notebooks or pipelines to access the modified EventHub Spark connector.

---

### Step 4: Create a Callback Function in Synapse

To integrate Azure Event Hubs with Synapse, create a callback function for authentication and managing the connection:

1. **Access Documentation**:
   - Visit the [AAD Authentication with Event Hubs Spark connector](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/use-aad-authentication-to-connect-eventhubs.md) for detailed instructions.

2. **Implement the Callback Function**:
   - Follow the instructions to authenticate Event Hubs via Azure Active Directory (AAD) using a callback function.
