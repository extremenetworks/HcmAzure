package com.extremenetworks.hcm.azure.mgr;

import java.io.ByteArrayOutputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.extremenetworks.hcm.azure.tools.NetworkInterfaceJsonSerializer;
import com.extremenetworks.hcm.azure.tools.NetworkJsonSerializer;
import com.extremenetworks.hcm.azure.tools.NetworkSecurityGroupJsonSerializer;
import com.extremenetworks.hcm.azure.tools.VirtualMachineJsonSerializer;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.network.NetworkInterface;
import com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.rabbitmq.client.Channel;

public class ResourcesWorker implements Runnable {

	private static final Logger logger = LogManager.getLogger(ResourcesWorker.class);
	private static ObjectMapper jsonMapper = new ObjectMapper();

	// Azure config
	private String appId;
	private String key;
	private String tenantId;
	private String subscription;

	// Rabbit MQ config
	private String RABBIT_QUEUE_NAME;
	private Channel rabbitChannel;

	// DB config
	private final String dbConnString = "jdbc:mysql://hcm-mysql:3306/Resources?useSSL=false";
	private final String dbUser = "root";
	private final String dbPassword = "password";

	// Helpers / Utilities
	private static final JsonFactory jsonFactory = new JsonFactory();
	private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public ResourcesWorker(String appId, String key, String tenantId, String subscription, String RABBIT_QUEUE_NAME,
			Channel rabbitChannel) {

		this.appId = appId;
		this.key = key;
		this.tenantId = tenantId;
		this.subscription = subscription;

		this.RABBIT_QUEUE_NAME = RABBIT_QUEUE_NAME;
		this.rabbitChannel = rabbitChannel;

		try {
			// load and register JDBC driver for MySQL
			Class.forName("com.mysql.jdbc.Driver");

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

		logger.debug("Starting Background worker to import data from Azure for app with ID " + appId);

		try {
			AzureManager azureManager = new AzureManager();
			boolean connected = azureManager.createConnection(appId, tenantId, key, AzureEnvironment.AZURE,
					subscription);

			if (!connected) {
				String msg = "Won't be able to retrieve any data from Azure since no authentication/authorization/connection could be established";
				logger.error(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			java.sql.Connection dbConn = DriverManager.getConnection(dbConnString, dbUser, dbPassword);

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

			writeToDb(dbConn, "Network", allNetworks);
			publishToRabbitMQ("Network", allNetworks);

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

			writeToDb(dbConn, "VM", allVMs);
			publishToRabbitMQ("VM", allVMs);

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

			writeToDb(dbConn, "SecurityGroup", allSecGroups);
			publishToRabbitMQ("SecurityGroup", allSecGroups);

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

			writeToDb(dbConn, "NetworkInterface", allNICs);
			publishToRabbitMQ("NetworkInterface", allNICs);

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
	private boolean writeToDb(java.sql.Connection dbConn, String resourceType, List<Object> data) {

		try {
			String sqlInsertStmtSubnets = "INSERT INTO azure (lastUpdated, appId, tenantId, subscription, resourceType, resourceData) "
					+ "VALUES (NOW(), ?, ?, ?, ?, ?) " + "ON DUPLICATE KEY UPDATE lastUpdated=NOW(), resourceData=?";

			PreparedStatement prepInsertStmtSubnets = dbConn.prepareStatement(sqlInsertStmtSubnets);

			prepInsertStmtSubnets.setString(1, appId);
			prepInsertStmtSubnets.setString(2, tenantId);
			prepInsertStmtSubnets.setString(3, subscription);
			prepInsertStmtSubnets.setString(4, resourceType);
			prepInsertStmtSubnets.setString(5, jsonMapper.writeValueAsString(data));
			prepInsertStmtSubnets.setString(6, jsonMapper.writeValueAsString(data));

			prepInsertStmtSubnets.executeUpdate();
			return true;

		} catch (Exception ex) {
			logger.error("Error trying to store resource data within the DB", ex);
			return false;
		}
	}

	private boolean publishToRabbitMQ(String resourceType, List<Object> data) {

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
			jsonGen.writeStringField("resourceType", resourceType);
			jsonGen.writeFieldName("resourceData");

			jsonGen.writeStartArray();
			jsonGen.writeRawValue(jsonMapper.writeValueAsString(data));
			jsonGen.writeEndArray();

			jsonGen.writeEndObject();

			jsonGen.writeEndArray();
			jsonGen.writeEndObject();

			jsonGen.close();
			outputStream.close();

			logger.debug("Forwarding updated list of " + resourceType + "s to the message queue " + RABBIT_QUEUE_NAME); // +
																														// ":
																														// "
																														// +
																														// outputStream.toString());
			rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, outputStream.toString().getBytes("UTF-8"));

			return true;

		} catch (Exception ex) {
			logger.error("Error trying to publish resource data to RabbitMQ", ex);
			return false;
		}

	}
}
