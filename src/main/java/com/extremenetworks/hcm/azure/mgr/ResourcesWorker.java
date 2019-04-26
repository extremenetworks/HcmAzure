package com.extremenetworks.hcm.azure.mgr;

import java.io.ByteArrayOutputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

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
import com.microsoft.azure.management.network.NetworkSecurityRule;
import com.microsoft.azure.management.network.NicIPConfiguration;
import com.microsoft.azure.management.network.Subnet;
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
    private final String SRC_SYS_TYPE = "Azure";
	private enum RESOURCE_TYPES { VM, Network, NwInterface, SecurityGroup }

	
	public ResourcesWorker(
			String appId, String key, String tenantId,	String subscription, 
			String RABBIT_QUEUE_NAME, Channel rabbitChannel) {
		
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
			boolean connected = azureManager.createConnection(
					appId, 
					tenantId, 
					key, 
					AzureEnvironment.AZURE,
					subscription);
			
			if (!connected) {
				String msg = "Won't be able to retrieve any data from Azure since no authentication/authorization/connection could be established";
				logger.error(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			java.sql.Connection dbConn = DriverManager.getConnection(dbConnString, dbUser, dbPassword);
			
			/*
			 * Networks (networks contain subnets)
			 */
			List<Object> allNetworks = azureManager.retrieveNetworks(appId);

			if (allNetworks == null) {
				String msg = "Error retrieving networks from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(dbConn, RESOURCE_TYPES.Network, allNetworks);
			publishBasicDataToRabbitMQ(RESOURCE_TYPES.Network, allNetworks);


			/*
			 * Security Groups
			 */
			List<Object> allSecGroups = azureManager.retrieveSecurityGroups(appId, "");

			if (allSecGroups == null) {
				String msg = "Error retrieving security groups from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(dbConn, RESOURCE_TYPES.SecurityGroup, allSecGroups);
			publishBasicDataToRabbitMQ(RESOURCE_TYPES.SecurityGroup, allSecGroups);


			/*
			 * Virtual Machines
			 */
			List<Object> allVMs = azureManager.retrieveVMs(appId);

			if (allVMs == null) {
				String msg = "Error retrieving VMs from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(dbConn, RESOURCE_TYPES.VM, allVMs);
			publishBasicDataToRabbitMQ(RESOURCE_TYPES.VM, allVMs);


			/*
			 * Network Interfaces
			 */
			List<Object> allNICs = azureManager.retrieveNetworkInterfaces(appId);

			if (allNICs == null) {
				String msg = "Error retrieving network interfaces from Azure - stopping any further processing";
				logger.warn(msg);
				rabbitChannel.basicPublish("", RABBIT_QUEUE_NAME, null, msg.getBytes("UTF-8"));
				return;
			}

			writeToDb(dbConn, RESOURCE_TYPES.NwInterface, allNICs);
			publishBasicDataToRabbitMQ(RESOURCE_TYPES.NwInterface, allNICs);


			logger.debug("Finished retrieving all resources from Azure app " + appId);

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
	private boolean writeToDb(java.sql.Connection dbConn, RESOURCE_TYPES resourceType, List<Object> data) {
		
		try {
			String sqlInsertStmtSubnets = 
				"INSERT INTO aws (lastUpdated, accountId, resourceType, resourceData) "
				+ "VALUES (NOW(), ?, ?, ?) "
				+ "ON DUPLICATE KEY UPDATE lastUpdated=NOW(), resourceData=?";
				
			PreparedStatement prepInsertStmtSubnets = dbConn.prepareStatement(sqlInsertStmtSubnets);
	        
			prepInsertStmtSubnets.setString(1, appId);
			prepInsertStmtSubnets.setString(2, resourceType.name());
			prepInsertStmtSubnets.setString(3, jsonMapper.writeValueAsString(data));
			prepInsertStmtSubnets.setString(4, jsonMapper.writeValueAsString(data));
			
			prepInsertStmtSubnets.executeUpdate();
			return true;
			
		} catch (Exception ex) {
			logger.error("Error trying to store resource data within the DB", ex);
			return false;
		}
	}
	

	private boolean publishBasicDataToRabbitMQ(RESOURCE_TYPES resourceType, List<Object> data) {

		if (data == null || data.isEmpty()) {	return false;	}
		
		try {
			Date now = new Date();
			String lastUpdate = dateFormatter.format(now);
			
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			JsonGenerator jsonGen = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);

			jsonGen.writeStartArray();
			
			if (resourceType == RESOURCE_TYPES.VM) {
				
				List<VirtualMachine> vms = (List<VirtualMachine>)(List<?>) data;
				generateJsonForVMs(jsonGen, vms, lastUpdate);
			}			

			else if (resourceType == RESOURCE_TYPES.SecurityGroup) {
				
				List<NetworkSecurityGroup> secGroups = (List<NetworkSecurityGroup>)(List<?>) data;
				generateJsonForSecGroups(jsonGen, secGroups, lastUpdate);
			}			

			else if (resourceType == RESOURCE_TYPES.Network) {
				
				List<Network> networks = (List<Network>)(List<?>) data;
				generateJsonForNetworks(jsonGen, networks, lastUpdate);
			}			

			else if (resourceType == RESOURCE_TYPES.NwInterface) {
				
				List<NetworkInterface> nics = (List<NetworkInterface>)(List<?>) data;
				generateJsonForNwInterfaces(jsonGen, nics, lastUpdate);
			}	

			jsonGen.writeEndArray();
			
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
	

	private void generateJsonForNwInterfaces(JsonGenerator jsonGen, List<NetworkInterface> nics, String lastUpdate) {
		
		try {
			for (NetworkInterface nic: nics) {
				
				jsonGen.writeStartObject();

				jsonGen.writeStringField("name", nic.name());
				jsonGen.writeStringField("srcSysType", SRC_SYS_TYPE);
				jsonGen.writeStringField("resourceType", RESOURCE_TYPES.NwInterface.name());
				jsonGen.writeStringField("id", nic.id());
				jsonGen.writeStringField("lastUpdate", lastUpdate);

				/* 
				 * Details
				 */
				jsonGen.writeArrayFieldStart("details");
				jsonGen.writeString("Region: " + nic.regionName());
				jsonGen.writeString("Resource Group: " + nic.resourceGroupName());
				
				String vm = nic.virtualMachineId().substring(nic.virtualMachineId().indexOf("/virtualMachines/") + "/virtualMachines/".length());
				jsonGen.writeString("Virtual Machine: " + vm);
				
				String secGroup = nic.networkSecurityGroupId().substring(nic.networkSecurityGroupId().indexOf("/networkSecurityGroups/") + "/networkSecurityGroups/".length());
				jsonGen.writeString("Security Group: " + secGroup);
				
				// IP Config
				if (nic.ipConfigurations() != null && !nic.ipConfigurations().isEmpty()) {
					
					// Take only the primary one for now
					Iterator<NicIPConfiguration> itIpConf = nic.ipConfigurations().values().iterator();
					NicIPConfiguration ipConf = itIpConf.next();
					
					String networkName = ipConf.networkId().substring(ipConf.networkId().indexOf("/virtualNetworks/") + "/virtualNetworks/".length());
					
					String publicIP = "";
					if (	nic.primaryIPConfiguration().getPublicIPAddress() != null && 
							nic.primaryIPConfiguration().getPublicIPAddress().ipAddress() != null && 
							!nic.primaryIPConfiguration().getPublicIPAddress().ipAddress().isEmpty()) {
						
						publicIP = nic.primaryIPConfiguration().getPublicIPAddress().ipAddress();
					}
					
					String ipConfDetails = "IP Config: Network: " + networkName + ", Subnet: " + 
							ipConf.subnetName() + ", Private IP: " + ipConf.privateIPAddress() + ", Public IP: " + publicIP;
					jsonGen.writeString(ipConfDetails);
				}
				
				// Tags
				if (nic.tags() != null && !nic.tags().isEmpty()) {
					String tags = "";
					Iterator<Entry<String, String>> itTags = nic.tags().entrySet().iterator();
					
					while (itTags.hasNext()) {
						Entry<String, String> tag = itTags.next();
						tags += tag.getKey() + " --> " + tag.getValue() + ", ";
					}
					
					tags = tags.substring(0, tags.length() - 2);
					jsonGen.writeString("Tags: " + tags);
				}

				
				// End the "details" array
				jsonGen.writeEndArray();
				
				// End the current network interface node
				jsonGen.writeEndObject();
			}
			
		} catch (Exception ex) {
			logger.error("Error generating JSON content for list of network interfaces",  ex);
		}	
	}

	
	private void generateJsonForVMs(JsonGenerator jsonGen, List<VirtualMachine> vms, String lastUpdate) {
		
		try {
			for (VirtualMachine vm: vms) {
				
				jsonGen.writeStartObject();

				jsonGen.writeStringField("name", vm.name());
				jsonGen.writeStringField("srcSysType", SRC_SYS_TYPE);
				jsonGen.writeStringField("resourceType", RESOURCE_TYPES.VM.name());
				jsonGen.writeStringField("id", vm.id());
				jsonGen.writeStringField("lastUpdate", lastUpdate);

				/* 
				 * Details
				 */
				jsonGen.writeArrayFieldStart("details");
				jsonGen.writeString("Region: " + vm.regionName());
				jsonGen.writeString("Resource Group: " + vm.resourceGroupName());
				jsonGen.writeString("Machine Type: " + vm.size().toString());
				
				if (vm.storageProfile() != null) {
					if (vm.storageProfile().osDisk() != null && vm.storageProfile().osDisk().osType() != null) {
						jsonGen.writeString("Device Family: " + vm.storageProfile().osDisk().osType().toString());	
					}
					
					if (vm.storageProfile().imageReference() != null) {
						jsonGen.writeString("Device Type: " + vm.storageProfile().imageReference().offer() + " " + vm.storageProfile().imageReference().sku());	
					}
				}			
				
				// Example: "PowerState/deallocated"
				String powerState = vm.powerState().toString();
				powerState = powerState.substring(powerState.indexOf("/") + 1);
				jsonGen.writeString("Power State: " + powerState);
				
				// Tags
				if (vm.tags() != null && !vm.tags().isEmpty()) {
					String tags = "";
					Iterator<Entry<String, String>> itTags = vm.tags().entrySet().iterator();
					
					while (itTags.hasNext()) {
						Entry<String, String> tag = itTags.next();
						tags += tag.getKey() + " --> " + tag.getValue() + ", ";
					}
					
					tags = tags.substring(0, tags.length() - 2);
					jsonGen.writeString("Tags: " + tags);
				}

				// Network Interface IDs
				if (vm.networkInterfaceIds() != null && !vm.networkInterfaceIds().isEmpty()) {
					String nicIDs = "";
					for (String id: vm.networkInterfaceIds()) {
						String idShort = id.substring(id.indexOf("/networkInterfaces/") + "/networkInterfaces/".length());
						nicIDs += idShort + ", ";
					}
					
					nicIDs = nicIDs.substring(0, nicIDs.length() - 2);
					jsonGen.writeString("Network Interface IDs: " + nicIDs);
				}

				
				// End the "details" array
				jsonGen.writeEndArray();
				
				// End the current virtual machine node
				jsonGen.writeEndObject();
			}
			
		} catch (Exception ex) {
			logger.error("Error generating JSON content for list of VMs",  ex);
		}	
	}

	
	private void generateJsonForNetworks(JsonGenerator jsonGen, List<Network> networks, String lastUpdate) {
		
		try {
			for (Network network: networks) {
				
				jsonGen.writeStartObject();

				jsonGen.writeStringField("name", network.name());
				jsonGen.writeStringField("srcSysType", SRC_SYS_TYPE);
				jsonGen.writeStringField("resourceType", RESOURCE_TYPES.Network.name());
				jsonGen.writeStringField("id", network.id());
				jsonGen.writeStringField("lastUpdate", lastUpdate);

				/* 
				 * Details
				 */
				jsonGen.writeArrayFieldStart("details");
				jsonGen.writeString("Region: " + network.regionName());
				jsonGen.writeString("Resource Group: " + network.resourceGroupName());
				
				// Tags
				if (network.tags() != null && !network.tags().isEmpty()) {
					String tags = "";
					Iterator<Entry<String, String>> itTags = network.tags().entrySet().iterator();
					
					while (itTags.hasNext()) {
						Entry<String, String> tag = itTags.next();
						tags += tag.getKey() + " --> " + tag.getValue() + ", ";
					}
					
					tags = tags.substring(0, tags.length() - 2);
					jsonGen.writeString("Tags: " + tags);
				}

				// Subnets
				if (network.subnets() != null && !network.subnets().isEmpty()) {
					
					jsonGen.writeString("Subnets");
					
					Iterator<Subnet> itSubnets = network.subnets().values().iterator();
					while (itSubnets.hasNext()) {
						
						Subnet subnet = itSubnets.next();
						
						String subnetDetails = "Name: " + subnet.name() + ", Address: " + subnet.addressPrefix() + ", IP Count: " + subnet.networkInterfaceIPConfigurationCount();
						if (subnet.networkSecurityGroupId() != null) {
							subnetDetails += ", Security Group: " + subnet.networkSecurityGroupId();
						}
						
						jsonGen.writeString(subnetDetails);
					}
				}
				
				// End the "details" array
				jsonGen.writeEndArray();
				
				// End the current network node
				jsonGen.writeEndObject();
			}
			
		} catch (Exception ex) {
			logger.error("Error generating JSON content for list of networks",  ex);
		}	
	}

	
	private void generateJsonForSecGroups(JsonGenerator jsonGen, List<NetworkSecurityGroup> secGroups, String lastUpdate) {
		
		try {
			for (NetworkSecurityGroup secGroup: secGroups) {
				
				jsonGen.writeStartObject();

				jsonGen.writeStringField("name", secGroup.name());
				jsonGen.writeStringField("srcSysType", SRC_SYS_TYPE);
				jsonGen.writeStringField("resourceType", RESOURCE_TYPES.SecurityGroup.name());
				jsonGen.writeStringField("id", secGroup.id());
				jsonGen.writeStringField("lastUpdate", lastUpdate);

				/* 
				 * Details
				 */
				jsonGen.writeArrayFieldStart("details");
				jsonGen.writeString("Region: " + secGroup.regionName());
				jsonGen.writeString("Resource Group: " + secGroup.resourceGroupName());
				
				// Tags
				if (secGroup.tags() != null && !secGroup.tags().isEmpty()) {
					String tags = "";
					Iterator<Entry<String, String>> itTags = secGroup.tags().entrySet().iterator();
					
					while (itTags.hasNext()) {
						Entry<String, String> tag = itTags.next();
						tags += tag.getKey() + " --> " + tag.getValue() + ", ";
					}
					
					tags = tags.substring(0, tags.length() - 2);
					jsonGen.writeString("Tags: " + tags);
				}

				// Default Rules
				if (secGroup.defaultSecurityRules() != null && !secGroup.defaultSecurityRules().isEmpty()) {
					
					jsonGen.writeString("Default Rules:");
					
					Iterator<NetworkSecurityRule> itDefaultRules = secGroup.defaultSecurityRules().values().iterator();
					while (itDefaultRules.hasNext()) {
						
						NetworkSecurityRule defaultRule = itDefaultRules.next();
						
						String defRuleDetails = "Name: " + defaultRule.name() + ", Description: " + defaultRule.description()
							+ ", " + defaultRule.access() + " " + defaultRule.direction() + ", Proto: " 
							+ defaultRule.protocol().toString() + ", Src Addr: " + defaultRule.sourceAddressPrefix() 
							+ ", Src Port: " + defaultRule.sourcePortRange() + ", Dst Addr: " + defaultRule.destinationAddressPrefix()
							+ ", Dst Port: " + defaultRule.destinationPortRange();
						
						jsonGen.writeString(defRuleDetails);
					}
				}

				// Security Rules
				if (secGroup.securityRules() != null && !secGroup.securityRules().isEmpty()) {
					
					jsonGen.writeString("Security Rules:");
					
					Iterator<NetworkSecurityRule> itSecRules = secGroup.securityRules().values().iterator();
					while (itSecRules.hasNext()) {
						
						NetworkSecurityRule secRule = itSecRules.next();
						
						String defRuleDetails = "Name: " + secRule.name() + ", Description: " + secRule.description()
							+ ", " + secRule.access() + " " + secRule.direction() + ", Proto: " 
							+ secRule.protocol().toString() + ", Src Addr: " + secRule.sourceAddressPrefix() 
							+ ", Src Port: " + secRule.sourcePortRange() + ", Dst Addr: " + secRule.destinationAddressPrefix()
							+ ", Dst Port: " + secRule.destinationPortRange();
						
						jsonGen.writeString(defRuleDetails);
					}
				}
				
				// End the "details" array
				jsonGen.writeEndArray();
				
				// End the current VPC node
				jsonGen.writeEndObject();
			}
			
		} catch (Exception ex) {
			logger.error("Error generating JSON content for list of VPCs",  ex);
		}	
	}

}
