package com.extremenetworks.hcm.azure.mgr;

import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Path("resources")
public class ResourceRes {

	private static final Logger logger = LogManager.getLogger(ResourceRes.class);
	private static ObjectMapper jsonMapper = new ObjectMapper();
	private static final JsonFactory jsonFactory = new JsonFactory();

	private final String rabbitServer = "rabbit-mq";
	private final static String RABBIT_QUEUE_NAME = "azure.resources";
	private static Channel rabbitChannel;

	private ExecutorService executor;

	private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final String DS_ENTITY_KIND_AZURE_RESOURCES = "AZURE_Resources";
	private final String DS_ENTITY_KIND_SRC_SYS_AZURE = "SourceSystemAzure";

	private Datastore datastore;

	public ResourceRes() {

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(rabbitServer);

			Connection connection = factory.newConnection();
			rabbitChannel = connection.createChannel();
			rabbitChannel.queueDeclare(RABBIT_QUEUE_NAME, false, false, false, null);

			executor = Executors.newCachedThreadPool();

			datastore = DatastoreOptions.getDefaultInstance().getService();

		} catch (Exception ex) {
			logger.error("Error setting up the 'Resources' resource", ex);
		}
	}

	/**
	 * Retrieves all resources (VMs, subnets, networks, etc.) for the given project
	 * ID from the DB
	 */
	@GET
	@Path("all")
	public String retrieveAllResources(@QueryParam("tenantId") String tenantId,
			@QueryParam("accountId") String accountId) {

		try {
			/* Retrieve the config for the given tenant & account from Datastore */
			AccountConfig accountConfig = new AccountConfig();
			String accountValidationMsg = retrieveAccountConfigFromDb(tenantId, accountId, accountConfig);

			if (!accountValidationMsg.isEmpty()) {
				return accountValidationMsg;
			}

			logger.debug("Retrieving all resource data for tenant " + tenantId + " and configured Azure account "
					+ accountId + " from GCP Datastore");

			// Retrieve all types of resources from GCP Datastore - SecuritGroups, VMs, etc.
			Query<Entity> queryResources = Query.newEntityQueryBuilder().setNamespace(tenantId)
					.setKind(DS_ENTITY_KIND_AZURE_RESOURCES)
					// .setFilter(CompositeFilter.and(PropertyFilter.eq("done", false),
					// PropertyFilter.eq("priority", 4)))
					.build();

			QueryResults<Entity> queryResourcesResults = datastore.run(queryResources);

			/*
			 * Start building the JSON string which contains some meta data. Example:
			 * 
			 * "dataType": "resources", "sourceSystemType": "azure",
			 * "sourceSystemProjectId": "418454969983",
			 */
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			JsonGenerator jsonGen = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);

			jsonGen.writeStartObject();

			jsonGen.writeStringField("dataType", "resources");
			jsonGen.writeStringField("sourceSystemType", "azure");
			jsonGen.writeStringField("sourceSystemAppId", accountConfig.getAppId());
			jsonGen.writeStringField("sourceSystemTenantId", accountConfig.getAzureTenantId());
			jsonGen.writeStringField("sourceSystemSubscription", accountConfig.getSubscription());

			/*
			 * The "data" field will contain an array of objects. Each object will contain
			 * all data on a particular resource type
			 */
			jsonGen.writeArrayFieldStart("data");

			while (queryResourcesResults.hasNext()) {

				Entity resourceDataEntity = queryResourcesResults.next();
				String resourceType = resourceDataEntity.getString("resourceType");

				if (resourceType != null && !resourceType.isEmpty()) {

					jsonGen.writeStartObject();

					/*
					 * Per resource type, the following meta data will be written (example):
					 * "lastUpdated": "2019-04-05 15:22:38", "resourceType": "Subnet",
					 * "resourceData": [ ... list of subnets ... ]
					 */
					jsonGen.writeStringField("lastUpdated",
							dateFormatter.format(resourceDataEntity.getTimestamp("lastUpdated").toDate()));
					jsonGen.writeStringField("resourceType", resourceType);

					// The list of subnets is already stored as a JSON string in the DB
					jsonGen.writeFieldName("resourceData");
					jsonGen.writeRawValue(resourceDataEntity.getString("resourceData"));

					jsonGen.writeEndObject();
				}

			}

			// Finalize the JSON string and output stream
			jsonGen.writeEndArray();
			jsonGen.writeEndObject();

			jsonGen.close();
			outputStream.close();

			return outputStream.toString();

		} catch (Exception ex) {
			logger.error(ex);
		}

		return "";
	}

	/**
	 * Starts a background worker that pulls all resources from the given account.
	 * This is a non-blocking REST call that just starts that worker in a separate
	 * thread and immediately responds to the caller. Once the background worker is
	 * done retrieving all data from AWS it will - update the DB - publish the data
	 * to RabbitMQ
	 * 
	 * @param accountId
	 * @param accessKeyId
	 * @param accessKeySecret
	 * @return
	 */
	@GET
	@Path("triggerUpdate")
	@Produces(MediaType.APPLICATION_JSON)
	public String triggerUpdateAllResources(@QueryParam("tenantId") String tenantId,
			@QueryParam("accountId") String accountId) {

		try {
			/* Retrieve the config for the given tenant & account from Datastore */
			AccountConfig accountConfig = new AccountConfig();
			String accountValidationMsg = retrieveAccountConfigFromDb(tenantId, accountId, accountConfig);

			if (!accountValidationMsg.isEmpty()) {
				return accountValidationMsg;
			}

			/* Config and start the background worker */
			logger.debug("Creating background worker to import data from Azure account: " + accountConfig.toString());

			executor.execute(new ResourcesWorker(tenantId, accountId, accountConfig, RABBIT_QUEUE_NAME, rabbitChannel,
					datastore));

			return jsonMapper.writeValueAsString(
					new ResourcesWebResponse(0, "Successfully triggered an update of all resource data"));

		} catch (Exception ex) {
			String msg = "General Error";
			logger.error(msg, ex);
			String returnValue;
			try {
				returnValue = jsonMapper.writeValueAsString(new ResourcesWebResponse(4, msg));
				return returnValue;
			} catch (Exception ex2) {
				return msg;
			}
		}
	}

	/**
	 * Retrieves the account config for the given tenant and account from Datastore.
	 * Stores the matching account config in the provided accountConfig parameter.
	 * Also validates the given tenantId and accountId params.
	 * 
	 * @param tenantId      Extreme Networks configured tenant id
	 * @param accountId     Extreme Networks configured account id
	 * @param accountConfig Empty, instantiated AccountConfig object that will be
	 *                      populated with the account config if found in Datastore
	 * @return An empty string if no error occured. If there was a problem, it
	 *         returns a JSON-configured string that can be used as an HTTP reponse.
	 */
	private String retrieveAccountConfigFromDb(String tenantId, String accountId, AccountConfig accountConfig) {

		try {
			if (tenantId == null || tenantId.isEmpty()) {

				String msg = "Missing URL parameter tenantId";
				logger.warn(msg);
				return jsonMapper.writeValueAsString(new ResourcesWebResponse(1, msg));
			}

			if (accountId == null || accountId.isEmpty()) {

				String msg = "Missing URL parameter accountId";
				logger.warn(msg);
				return jsonMapper.writeValueAsString(new ResourcesWebResponse(2, msg));
			}

			// Retrieve all configured GCP source systems for this tenant
			logger.debug("Retrieving config for tenant id " + tenantId + " and account id " + accountId);

			Query<Entity> query = Query.newEntityQueryBuilder().setNamespace(tenantId)
					.setKind(DS_ENTITY_KIND_SRC_SYS_AZURE).build();

			QueryResults<Entity> queryResults = datastore.run(query);

			while (queryResults.hasNext()) {

				Entity srcSysEntity = queryResults.next();
				String srcSysAccountId = srcSysEntity.getKey().getName();

				// Try to match the given account id with the configured account (Entity key
				// name == accountId)
				if (accountId.equals(srcSysAccountId)) {

					if (srcSysEntity.isNull("appId") || srcSysEntity.isNull("key") || srcSysEntity.isNull("tenantId")
							|| srcSysEntity.isNull("subscription")) {
						String msg = "Found account config but it is missing one or more properties";
						logger.warn(msg);
						return jsonMapper.writeValueAsString(new ResourcesWebResponse(3, msg));
					}

					if (srcSysEntity.getString("appId").isEmpty() || srcSysEntity.getString("key").isEmpty()
							|| srcSysEntity.getString("tenantId").isEmpty()
							|| srcSysEntity.getString("subscription").isEmpty()) {
						String msg = "Found account config but one or more properties are empty";
						logger.warn(msg);
						return jsonMapper.writeValueAsString(new ResourcesWebResponse(4, msg));
					}

					accountConfig.setTenantId(tenantId);
					accountConfig.setAccountId(accountId);
					accountConfig.setAppId(srcSysEntity.getString("appId"));
					accountConfig.setKey(srcSysEntity.getString("key"));
					accountConfig.setSubscription(srcSysEntity.getString("subscription"));
					accountConfig.setAzureTenantId(srcSysEntity.getString("tenantId"));

					logger.debug("Found configured Azure source system: " + accountConfig.toString());
					return "";
				}
			}

			String msg = "Could not find a configured Azure source system for tenant id " + tenantId
					+ " and account id " + accountId;
			logger.warn(msg);
			return jsonMapper.writeValueAsString(new ResourcesWebResponse(5, msg));

		} catch (Exception ex) {
			String msg = "General Error";
			logger.error(msg, ex);
			String returnValue;
			try {
				returnValue = jsonMapper.writeValueAsString(new ResourcesWebResponse(6, msg));
				return returnValue;
			} catch (Exception ex2) {
				return msg;
			}
		}
	}
}