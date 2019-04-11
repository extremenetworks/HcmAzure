package com.extremenetworks.hcm.gcp.mgr;

import java.io.ByteArrayOutputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.compute.model.ZoneList;
import com.rabbitmq.client.Channel;


public class ResourcesWorker implements Runnable {

	private static final Logger logger = LogManager.getLogger(ResourcesWorker.class);
	private static ObjectMapper jsonMapper = new ObjectMapper();
	
	// GCP config
	private String projectId;
	private String authenticationFileName;

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
	
	
	public ResourcesWorker(
			String projectId, String authenticationFileName, String RABBIT_QUEUE_NAME, Channel rabbitChannel) {
		
		this.projectId = projectId;
		this.authenticationFileName = authenticationFileName;
		
		this.RABBIT_QUEUE_NAME = RABBIT_QUEUE_NAME;
		this.rabbitChannel = rabbitChannel;

		try {
			// load and register JDBC driver for MySQL 
	        Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception ex) {
			logger.error("Error loading mysql jdbc driver within the worker class", ex);
		}
	}
	
	
	@Override
	public void run() {
		
		logger.debug("Starting Background worker to import data from GCP for project with ID " + projectId + " using auth file " + authenticationFileName);
		
		try {	
			GoogleComputeEngineManager computeManager = new GoogleComputeEngineManager();
			boolean connected = computeManager.createComputeConnection(projectId, authenticationFileName);
			
			if (!connected) {
				String msg = "Won't be able to retrieve any data from Google Compute Engine since no authentication/authorization/connection could be established";
				logger.error(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			java.sql.Connection dbConn = DriverManager.getConnection(dbConnString, dbUser, dbPassword);
			
			/* Import zones */
			ZoneList allZones = computeManager.retrieveAllZones(projectId, "");
			if (allZones == null || allZones.getItems() == null) {
				String msg = "Won't be able to retrieve any instance data from Google Compute Engine since no zones could be retrieved";
				logger.error(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}
	
			String result = jsonMapper.writeValueAsString(allZones.getItems());

			rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, result.getBytes("UTF-8"));
	        
		} catch (Exception ex) {
			logger.error(ex);
			return;
		}
	}
	

	/** Writes the given data (Subnets, VMs, etc.) to the DB
	 * 
	 * @param dbConn		Active DB connection
	 * @param resourceType 	Valid types: Subnet, VM, 
	 * @param data			Map of resource data. The values can contain any type of object (subnets, VMs, etc.) 
	 * 						and will be written to JSON data and then stored in the DB
	 * @return
	 */
	private boolean writeToDb(java.sql.Connection dbConn, String resourceType, HashMap<String, Object> data) {
	
		try {
			String sqlInsertStmtSubnets = 
				"INSERT INTO aws (lastUpdated, projectId, resourceType, resourceData) "
				+ "VALUES (NOW(), ?, ?, ?) "
				+ "ON DUPLICATE KEY UPDATE lastUpdated=NOW(), resourceData=?";
				
			PreparedStatement prepInsertStmtSubnets = dbConn.prepareStatement(sqlInsertStmtSubnets);
	        
			prepInsertStmtSubnets.setString(1, projectId);
			prepInsertStmtSubnets.setString(2, resourceType);
			prepInsertStmtSubnets.setString(3, jsonMapper.writeValueAsString(data.values()));
			prepInsertStmtSubnets.setString(4, jsonMapper.writeValueAsString(data.values()));
			
			prepInsertStmtSubnets.executeUpdate();
			return true;
			
		} catch (Exception ex) {
			logger.error("Error trying to store resource data within the DB", ex);
			return false;
		}
	}
	
	
	private boolean publishToRabbitMQ(String resourceType, HashMap<String, Object> data) {
		
		try {
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	        JsonGenerator jsonGen = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);

	        jsonGen.writeStartObject();
	        
	        jsonGen.writeStringField("dataType", "resources");
	        jsonGen.writeStringField("sourceSystemType", "gcp");
	        jsonGen.writeStringField("sourceSystemProjectId", projectId);
	        		
	        jsonGen.writeArrayFieldStart("data");
	        
			Date now = new Date();
			
			jsonGen.writeStartObject();
	        
	        jsonGen.writeStringField("lastUpdated", dateFormatter.format(now));
			jsonGen.writeStringField("resourceType", resourceType);
	        jsonGen.writeFieldName("resourceData");
	        
	        jsonGen.writeStartArray();
	        jsonGen.writeRawValue(jsonMapper.writeValueAsString(data.values()));
	        jsonGen.writeEndArray();
	        
	        jsonGen.writeEndObject();
			
	        jsonGen.writeEndArray();
	        jsonGen.writeEndObject();
	        
	        jsonGen.close();        
	        outputStream.close();
	        
			logger.debug("Forwarding the list of subnets to the message queue " + RABBIT_QUEUE_NAME); //+ ": " + outputStream.toString());
			rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, outputStream.toString().getBytes("UTF-8"));
			
			return true;
			
		} catch (Exception ex) {
			logger.error("Error trying to publish resource data to RabbitMQ", ex);
			return false;
		}
		
	}
}
