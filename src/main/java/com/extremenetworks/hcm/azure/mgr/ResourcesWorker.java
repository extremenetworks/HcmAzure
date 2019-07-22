package com.extremenetworks.hcm.azure.mgr;

import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.extremenetworks.hcm.azure.tools.NetworkInterfaceJsonSerializer;
import com.extremenetworks.hcm.azure.tools.NetworkJsonSerializer;
import com.extremenetworks.hcm.azure.tools.NetworkSecurityGroupJsonSerializer;
import com.extremenetworks.hcm.azure.tools.VirtualMachineJsonSerializer;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.network.NetworkInterface;
import com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.rabbitmq.client.Channel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourcesWorker implements Runnable {

	private static final Logger logger = LogManager.getLogger(ResourcesWorker.class);
	private static ObjectMapper jsonMapper = new ObjectMapper();

	// Extreme Networks GCP tenant id
	String tenantId;
	String accountId;

	// Azure config
	private String appId;
	private String key;
	private String azureTenantId;
	private String subscription;

	// Rabbit MQ config
	private String RABBIT_QUEUE_NAME;
	private Channel rabbitChannel;

	// GCP Datastore
	private Datastore datastore;
	private final String DS_ENTITY_KIND_AZURE_RESOURCES = "AZURE_Resources";

	// Helpers / Utilities
	private static final JsonFactory jsonFactory = new JsonFactory();
	private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private enum RESOURCE_TYPES {
		VM, SecurityGroup, Network, NetworkInterface
	}

	public ResourcesWorker(String tenantId, String accountId, String appId, String key, String azureTenantId,
			String subscription, String RABBIT_QUEUE_NAME, Channel rabbitChannel, Datastore datastore) {

		// Extreme Networks GCP tenant id
		this.tenantId = tenantId;
		this.accountId = accountId;

		// Azure account
		this.appId = appId;
		this.key = key;
		this.azureTenantId = azureTenantId;
		this.subscription = subscription;

		this.RABBIT_QUEUE_NAME = RABBIT_QUEUE_NAME;
		this.rabbitChannel = rabbitChannel;

		this.datastore = datastore;

		try {
			SimpleModule azureModule = new SimpleModule("AzureModule");
			azureModule.addSerializer(NetworkInterface.class, new NetworkInterfaceJsonSerializer());
			azureModule.addSerializer(VirtualMachine.class, new VirtualMachineJsonSerializer());
			azureModule.addSerializer(Network.class, new NetworkJsonSerializer());
			azureModule.addSerializer(NetworkSecurityGroup.class, new NetworkSecurityGroupJsonSerializer());
			jsonMapper.registerModule(azureModule);

		} catch (Exception ex) {
			logger.error("Error loading mysql jdbc driver within the worker class", ex);
		}
	}

	@Override
	public void run() {

		logger.debug("Starting Background worker to import data from Azure using app id " + appId + ", tenant "
				+ azureTenantId + ", subscription " + subscription + " and key " + key);

		try {
			AzureManager azureManager = new AzureManager();
			boolean connected = azureManager.createConnection(appId, azureTenantId, key, AzureEnvironment.AZURE,
					subscription);

			if (!connected) {
				String msg = "Won't be able to retrieve any data from Azure since no authentication/authorization/connection could be established";
				logger.error(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			/*
			 * Retrieve networks (contain subnets)
			 */
			List<Object> allNetworks = azureManager.retrieveNetworks(appId);

			if (allNetworks == null) {
				String msg = "Error retrieving networks from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(RESOURCE_TYPES.Network, allNetworks);
			publishToRabbitMQ(RESOURCE_TYPES.Network, allNetworks);

			/*
			 * Retrieve VMs
			 */
			List<Object> allVMs = azureManager.retrieveVMs(appId);

			if (allVMs == null) {
				String msg = "Error retrieving VMs from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(RESOURCE_TYPES.VM, allVMs);
			publishToRabbitMQ(RESOURCE_TYPES.VM, allVMs);

			/*
			 * Retrieve Security Groups
			 */
			List<Object> allSecGroups = azureManager.retrieveSecurityGroups(appId, "");

			if (allSecGroups == null) {
				String msg = "Error retrieving security groups from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(RESOURCE_TYPES.SecurityGroup, allSecGroups);
			publishToRabbitMQ(RESOURCE_TYPES.SecurityGroup, allSecGroups);

			/*
			 * Retrieve Network Interfaces
			 */
			List<Object> allNICs = azureManager.retrieveNetworkInterfaces(appId);

			if (allNICs == null) {
				String msg = "Error retrieving network interfaces from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(RESOURCE_TYPES.NetworkInterface, allNICs);
			publishToRabbitMQ(RESOURCE_TYPES.NetworkInterface, allNICs);

			logger.debug("Finished retrieving all resources from Azure app " + appId);

		} catch (Exception ex) {
			logger.error(ex);
			return;
		}
	}

	/**
	 * Writes the given data (Subnets, VMs, etc.) to the DB
	 * 
	 * @param dbConn       Active DB connection
	 * @param resourceType Valid types: Subnet, VM,
	 * @param data         Map of resource data. The values can contain any type of
	 *                     object (subnets, VMs, etc.) and will be written to JSON
	 *                     data and then stored in the DB
	 * @return
	 */
	private boolean writeToDb(RESOURCE_TYPES resourceType, List<Object> data) {

		try {
			// The name/ID for the new entity
			String name = resourceType.name();

			// The Cloud Datastore key for the new entity
			Key entityKey = datastore.newKeyFactory().setNamespace(tenantId).setKind(DS_ENTITY_KIND_AZURE_RESOURCES)
					.newKey(name);

			Entity dataEntity = Entity
					.newBuilder(entityKey).set("lastUpdated", Timestamp.now()).set("accountId", accountId)
					.set("resourceType", resourceType.name()).set("resourceData", StringValue
							.newBuilder(jsonMapper.writeValueAsString(data)).setExcludeFromIndexes(true).build())
					.build();

			logger.debug("About to update / write this entity towards GCP datastore:"
					+ jsonMapper.writeValueAsString(dataEntity));

			// Saves the entity
			datastore.put(dataEntity);

			return true;

		} catch (Exception ex) {
			logger.error("Error trying to store resource data within GCP Datastore", ex);
			return false;
		}
	}

	private boolean publishToRabbitMQ(RESOURCE_TYPES resourceType, List<Object> data) {

		try {
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			JsonGenerator jsonGen = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);

			jsonGen.writeStartObject();

			jsonGen.writeStringField("dataType", "resources");
			jsonGen.writeStringField("sourceSystemType", "azure");
			jsonGen.writeStringField("sourceSystemProjectId", appId);

			jsonGen.writeArrayFieldStart("data");

			Date now = new Date();

			jsonGen.writeStartObject();

			jsonGen.writeStringField("lastUpdated", dateFormatter.format(now));
			jsonGen.writeStringField("resourceType", resourceType.name());
			jsonGen.writeFieldName("resourceData");

			jsonGen.writeStartArray();
			jsonGen.writeRawValue(jsonMapper.writeValueAsString(data));
			jsonGen.writeEndArray();

			jsonGen.writeEndObject();

			jsonGen.writeEndArray();
			jsonGen.writeEndObject();

			jsonGen.close();
			outputStream.close();

			logger.debug("Forwarding updated list of " + resourceType + "s to the message queue " + RABBIT_QUEUE_NAME);
			rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, outputStream.toString().getBytes("UTF-8"));

			return true;

		} catch (Exception ex) {
			logger.error("Error trying to publish resource data to RabbitMQ", ex);
			return false;
		}

	}
}
