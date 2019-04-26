package com.extremenetworks.hcm.azure.mgr;

import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.extremenetworks.hcm.azure.tools.NetworkInterfaceJsonSerializer;
import com.extremenetworks.hcm.azure.tools.NetworkJsonSerializer;
import com.extremenetworks.hcm.azure.tools.NetworkSecurityGroupJsonSerializer;
import com.extremenetworks.hcm.azure.tools.VirtualMachineJsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.PagedList;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.network.NetworkInterface;
import com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.microsoft.rest.RestClient;
import com.microsoft.rest.ServiceResponseBuilder;
import com.microsoft.rest.serializer.JacksonAdapter;

import okhttp3.OkHttpClient;


public class AzureManager {

	private static final Logger logger = LogManager.getLogger(AzureManager.class);
	
	ObjectMapper jsonMapper = new ObjectMapper();
	//private int maxResultsGetEc2Instances = 100;

	//private HashSet<String> regionsWithConnectionErrors;
	private boolean enableDebugRestClient = false;
	
	/* One connection config per Azure account */
	private HashMap<String, Azure> azureConnections;
	private final Pattern patternIpAddress = Pattern.compile(
		"^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");
	
	
	public AzureManager() {
		
		//regionsWithConnectionErrors = new HashSet<String>();
		
		/* Needed for serializing the Azure classes */
//		jsonMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
//		jsonMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		
		azureConnections = new HashMap<String, Azure>();
		
		SimpleModule azureModule = new SimpleModule("AzureModule");
		azureModule.addSerializer(NetworkInterface.class, new NetworkInterfaceJsonSerializer());
		azureModule.addSerializer(VirtualMachine.class, new VirtualMachineJsonSerializer());
		azureModule.addSerializer(Network.class, new NetworkJsonSerializer());
		azureModule.addSerializer(NetworkSecurityGroup.class, new NetworkSecurityGroupJsonSerializer());
		jsonMapper.registerModule(azureModule);
	}
	
	
	/** Create a new connection object based on the provided parameters and stores it in a cache 
	 * that is keyed-off of the given account name. If that cache already holds an existing 
	 * connection config for that account name then it will be overwritten / updated */
	public boolean createConnection(String appId, String tenantId, String key, AzureEnvironment azureEnvironment, String subscription) {
		
		if (appId == null || appId.isEmpty()) {
			logger.warn("Cannot create Azure connection since no application id is provided");
			return false;
		}
		if (tenantId == null || tenantId.isEmpty()) {
			logger.warn("Cannot create Azure connection since no tenant id is provided");
			return false;
		}
		if (key == null || key.isEmpty()) {
			logger.warn("Cannot create Azure connection since no key is provided");
			return false;
		}
		if (azureEnvironment == null) {
			logger.warn("Cannot create Azure connection since no azure environment is provided");
			return false;
		}
		if (subscription == null || subscription.isEmpty()) {
			logger.warn("Cannot create Azure connection since no subscription is provided");
			return false;
		}
		
		ApplicationTokenCredentials credentials = new ApplicationTokenCredentials(
				appId, 
				tenantId,
		        key, 
		        AzureEnvironment.AZURE);

//		Azure azureConnection = Azure
//		        .configure()
//		        .withLogLevel(LogLevel.NONE)
//		        .authenticate(credentials)
//		        .withSubscription(subscription);
	
		
		OkHttpClient.Builder httpClientBuilder = buildAzureRestClient(10000, 10000);
		
		retrofit2.Retrofit.Builder retrofitBuilder = new retrofit2.Retrofit.Builder();
		retrofitBuilder.baseUrl("https://management.azure.com");

		com.microsoft.rest.RestClient restClient = null;
		
		if (enableDebugRestClient) {
			restClient = new RestClient.Builder(httpClientBuilder, retrofitBuilder)
				.withCredentials(credentials)
				.withBaseUrl("https://management.azure.com")
				.withResponseBuilderFactory(new ServiceResponseBuilder.Factory())
				.withSerializerAdapter(new JacksonAdapter())
				.withLogLevel(com.microsoft.rest.LogLevel.BODY_AND_HEADERS) // Logging enabled
				.build();
			
		} else {
			restClient = new RestClient.Builder(httpClientBuilder, retrofitBuilder)
					.withCredentials(credentials)
					.withBaseUrl("https://management.azure.com")
					.withResponseBuilderFactory(new ServiceResponseBuilder.Factory())
					.withSerializerAdapter(new JacksonAdapter())
					.build();
		}
		
		Azure azureConnection = Azure.authenticate(restClient, tenantId).withSubscription(subscription);
		
		azureConnections.put(appId, azureConnection);
				
		return true;
	}
	

	/** Initializes the global member awsClientConfig. This is required before any interaction with the AWS service!
	 * The method tries to load the custom Java trustStore that holds the Amazon Root CA certificate.
	 * This trustStore is used to build a custom SSLContext which is then loaded into the custom client config.
	 * This is then used in all calls to the AWS library that interact with the AWS cloud to ensure they trust any
	 * Amazon server certificates.
	 * 
	 * @param socketTimeout		Timeout in milliseconds to wait for the socket to any AWS API server to setup
	 * @param connectionTimeout	Timeout in milliseconds to wait for a result on any API request towards any AWS server
	 * @return	True in case of success, False in case of any error.
	 */
	private OkHttpClient.Builder buildAzureRestClient(int socketTimeout, int connectionTimeout) {

		
		
		try {
			/* Load the keyStore file.
			 * It is located within the WAR file under /WEB-INF/classes 
			 * which is a default folder for the classloader to search for files to load.
			 * Within the project structure, the file is located under src/main/resources */
			ClassLoader classLoader = AzureManager.class.getClassLoader();
			InputStream azureKeyStoreStream = classLoader.getResourceAsStream("azureJavaKeyStore");
			
			// String awsKeyStorePath = System.getProperty("jboss.server.config.dir") + "/connect/awsJavaKeyStore";
			// InputStream awskeyStoreStream = new FileInputStream(awsKeyStorePath);
						
			if (azureKeyStoreStream == null) {
				logger.error("Couldn't find custom Azure Java trust store file: azureJavaKeyStore - won't be able to communicate with the Azure cloud!");
				return null;
			}
			
			
			// Create a custom keystore and load the content of the azureJavaKeyStore into it
			KeyStore azureKeyStore = KeyStore.getInstance("JKS");
			azureKeyStore.load(azureKeyStoreStream, "changeit".toCharArray());
			
			TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); // PKIX
			tmf.init(azureKeyStore); // if you pass null, you get the JVM defaults
			                        // which is CACerts file or javax.net.ssl.trustStore
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); // PKIX
			kmf.init(azureKeyStore, "changeit".toCharArray()); // if you pass null, you get the JVM defaults
			                        // which is CACerts file or javax.net.ssl.trustStore

			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
			
			TrustManager[] trustManagers = tmf.getTrustManagers();
			if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
				throw new IllegalStateException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
			}
			X509TrustManager trustManager = (X509TrustManager) trustManagers[0];
			//SSLContext sslContext = SSLContext.getInstance("SSL");
			//sslContext.init(null, new TrustManager[] { trustManager }, null);
			
			
			if (trustManager.getAcceptedIssuers() != null && trustManager.getAcceptedIssuers().length > 0) {
				
				for (X509Certificate issuer : trustManager.getAcceptedIssuers()) {
					logger.debug("TrustManager accepted issuer: DN: " + issuer.getIssuerDN()+ ", subject DN: " + issuer.getSubjectDN());
				}
				
			} else {
				logger.debug("TrustManager has no accepted issuers");
			}
			
			
			SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
			
			OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder().sslSocketFactory(sslSocketFactory, trustManager);
			
			return httpClientBuilder;
			
		} catch (Exception ex) {		
			logger.error("Error building the Java trust store and SSL socket factory", ex);
			return null;
		}
	}
	

	/** Returns the list of all existing "managed" security groups from the given Azure account. Uses a special tag ("ExtremePolicyId")
	 * as a filter on each security group to limit the result to only those groups that are synchronized by Connect.
	 * @param	resourceGroupName	Optional. If provided, the method will return only managed security groups from the given resource group
	 */
	public HashMap<String, NetworkSecurityGroup> retrieveManagedSecurityGroups(String accountName, String resourceGroupName) {

		Azure azureConnection = azureConnections.get(accountName);
		if (azureConnection == null) {
			logger.warn("Cannot retrieve the managed security groups since there is no Azure connection for account " + accountName);
			return null;
		}
		
		logger.debug("Trying to retrieve managed security groups from Azure account " + accountName); // + " - ignoring regions: " +  awsConnection.getRegionsToIgnore());
		
		
		HashMap<String, NetworkSecurityGroup> secGroups = new HashMap<String, NetworkSecurityGroup>();
		
		try {
			int countTotalGroups = 0;
			PagedList<NetworkSecurityGroup> securityGroups = azureConnection.networkSecurityGroups().list();
			
			for (NetworkSecurityGroup secGroup: securityGroups) {
				
				countTotalGroups++;
				
				/* The 'ExtremePolicyId' tag indicates a managed group */
				if (secGroup.tags().keySet().contains("ExtremePolicyId")) {
					
					if (resourceGroupName != null && !resourceGroupName.isEmpty()) {
						
						if (secGroup.resourceGroupName().equalsIgnoreCase(resourceGroupName)) {
							logger.debug("Retrieved next managed sec group that is also part of resource group " + resourceGroupName 
								+ ": " + jsonMapper.writeValueAsString(secGroup));
							secGroups.put(secGroup.id(), secGroup);	
						} else {
							logger.debug("Retrieved next managed sec group " + secGroup.name() + " but it is not part of resource group " 
								+ resourceGroupName + " (" + secGroup.resourceGroupName() + ") - ignoring");
						}
						
					} else {
						logger.debug("Retrieved next managed sec group " + jsonMapper.writeValueAsString(secGroup));
						secGroups.put(secGroup.id(), secGroup);	
					}
					
				} else {
					logger.debug("Retrieved unmanaged sec group " + secGroup.name() + " - ignoring");
				}
			}	
			
			logger.debug("Found a total of " + countTotalGroups + " security groups from account " + accountName
				+ " out of which " + secGroups.size() + " managed groups got imported");
			return secGroups;
			
		} catch (Exception ex) {
			logger.error("Error retrieving security groups from account " + accountName, ex);
			return null;
		}
	}
	

	/** Returns the list of all existing "security groups from the given Azure account. 
	 * @param	resourceGroupName	Optional. If provided, the method will return only security groups from the given resource group
	 */
	public List<Object> retrieveSecurityGroups(String accountName, String resourceGroupName) {

		Azure azureConnection = azureConnections.get(accountName);
		if (azureConnection == null) {
			logger.warn("Cannot retrieve the security groups since there is no Azure connection for account " + accountName);
			return null;
		}
		
		logger.debug("Trying to retrieve security groups from Azure account " + accountName); // + " - ignoring regions: " +  awsConnection.getRegionsToIgnore());
		
		List<Object> secGroups = new ArrayList<Object>();
		
		try {
			int countTotalGroups = 0;
			PagedList<NetworkSecurityGroup> securityGroups = azureConnection.networkSecurityGroups().list();
			
			for (NetworkSecurityGroup secGroup: securityGroups) {
				
				countTotalGroups++;
			
				if (resourceGroupName != null && !resourceGroupName.isEmpty()) {
					
					if (secGroup.resourceGroupName().equalsIgnoreCase(resourceGroupName)) {
						logger.debug("Retrieved next sec group that is also part of resource group " + resourceGroupName 
							+ ": " + jsonMapper.writeValueAsString(secGroup));
						secGroups.add(secGroup);	
					} else {
						logger.debug("Retrieved next sec group " + secGroup.name() + " but it is not part of resource group " 
							+ resourceGroupName + " (" + secGroup.resourceGroupName() + ") - ignoring");
					}
					
				} else {
					logger.debug("Retrieved next sec group " + jsonMapper.writeValueAsString(secGroup));
					secGroups.add(secGroup);	
				}
			}	
			
			logger.debug("Found a total of " + countTotalGroups + " security groups from account " + accountName
				+ " out of which " + secGroups.size() + " groups got imported");
			return secGroups;
			
		} catch (Exception ex) {
			logger.error("Error retrieving security groups from account " + accountName, ex);
			return null;
		}
	}
	

	/** Returns a single security group from Azure based on the provided groupId. 
	 * @param	groupId	The id of the security group to retrieve. Will be used as the only filter for the request
	 * @return	The Azure security group matching the provided security group id or null
	 */
	public NetworkSecurityGroup retrieveSecurityGroup(String accountName, String groupId) {

		if (groupId == null || groupId.isEmpty()) {
			return null;
		}
		
		Azure azureConnection = azureConnections.get(accountName);
		if (azureConnection == null) {
			logger.warn("Cannot retrieve the security group with id " + groupId + " since there is no Azure connection for account " + accountName);
			return null;
		}
		
		logger.debug("Trying to retrieve security group with id " + groupId + " from account " + accountName);
		
	
		try {
			NetworkSecurityGroup securityGroup = azureConnection.networkSecurityGroups().getById(groupId);
        	
            logger.debug("Successfully retrieved security group with id " + groupId + " (account " + accountName + "): " + 
            		jsonMapper.writeValueAsString(securityGroup));
            return securityGroup;
            
		} catch (Exception ex) {
			logger.error("Error retrieving security group with id " + groupId + " from account " + accountName + ": ", ex);
			return null;
		}
	}

	
		
	
	/** Retrieves all virtual machines.
	 * @return	The full list of all VMs. HashMap key: VM ID in lower-case! Azure provides the VM id in slightly different cases depending on 
	 * where it is provided: When retrieving a VM, the resource group part of the id is upper-case. But the vm id reference within a 
	 * network interface uses a lower-case resource group. To quickly find the corresponding VM within the cache all code should use the
	 * lower-case version of the id. 
	 */
	public List<Object> retrieveVMs(String accountName) {

		if (accountName == null || accountName.isEmpty()) {
			logger.warn("Cannot retrieve virtual machines since the given account name or security group id is empty");
			return null;
		}
		
		Azure azureConnection = azureConnections.get(accountName);
		if (azureConnection == null) {
			logger.warn("Cannot retrieve virtual machines since there is no Azure connection for account " + accountName);
			return null;
		}

		List<Object> allInstances = new ArrayList<Object>();		
	
		try {
			PagedList<VirtualMachine> vmList = azureConnection.virtualMachines().list();
			
			allInstances.addAll(vmList);
			
			logger.debug("Successfully retrieved " + allInstances.size() + " from account " + accountName);
			return allInstances;
			
		} catch (Exception ex) {
			logger.error("Error retrieving instances from account " + accountName, ex);
			return null;
		}
	}
	

	/** Retrieves all network interfaces from all VMs.
	 * @return	The full list of all network interfaces. HashMap key: nic ID
	 */
	public List<Object> retrieveNetworkInterfaces(String accountName) {

		if (accountName == null || accountName.isEmpty()) {
			logger.warn("Cannot network interfaces since the given account name is empty");
			return null;
		}
		
		Azure azureConnection = azureConnections.get(accountName);
		if (azureConnection == null) {
			logger.warn("Cannot retrieve network interfaces since there is no Azure connection for account " + accountName);
			return null;
		}

		List<Object> allNICs = new ArrayList<Object>();		
	
		try {
			PagedList<NetworkInterface> nicList = azureConnection.networkInterfaces().list();
			
			for (NetworkInterface nic: nicList) {
				allNICs.add(nic);
			}	
			
			logger.debug("Successfully retrieved " + allNICs.size() + " network interfaces from account " + accountName);
			return allNICs;
			
		} catch (Exception ex) {
			logger.error("Error retrieving network interfaces from account " + accountName, ex);
			return null;
		}
	}
	
	
	/** Retrieves all networks from the given account.
	 * The HashMap's key is the network id in lower-case
	 */
	public List<Object> retrieveNetworks(String accountName) {

		if (accountName == null || accountName.isEmpty()) {
			logger.warn("Cannot retrieve networks since the given account name is empty");
			return null;
		}
		
		Azure azureConnection = azureConnections.get(accountName);
		if (azureConnection == null) {
			logger.warn("Cannot retrieve networks since there is no Azure connection for account " + accountName);
			return null;
		}
		
		logger.debug("Trying to retrieve all networks from Azure account " + accountName);
		
		
		List<Object> allNetworks = new ArrayList<Object>();
			
		try {
			PagedList<Network> networkList = azureConnection.networks().list();
			
			allNetworks.addAll(networkList);
			
			logger.debug("Retrieved all " + allNetworks.size() + " networks from account " + accountName);
			return allNetworks;
			
		} catch (Exception ex) {
			logger.error("Error retrieving networks from account " + accountName, ex);
			return null;
		}
	}
	
	
	/** Assigns the given security group to the given instance interface. Azure does not support
	 * assigning multiple security groups to an interface.
	 * 
	 * @param instanceId	The instance interface to modify the security group for
	 * @param secGroup		The security group to assign to the instance interface. If null is provided, any associated security group
	 * 						will be removed from that interface
	 */
	public boolean assignSecurityGroupToNwInterface(String accountName, NetworkInterface nwIf, NetworkSecurityGroup secGroup) {

		if (accountName == null || accountName.isEmpty()) {
			logger.warn("Cannot assign a security group to a network interface since the given account name is empty");
			return false;
		}
		if (nwIf == null) {
			logger.warn("Cannot assign a security group to a network interface since there is no network interface provided for " + accountName);
			return false;
		}
		
		Azure azureConnection = azureConnections.get(accountName);
		if (azureConnection == null) {
			logger.warn("Cannot assign a security group to a network interface since there is no Azure connection for account " + accountName);
			return false;
		}
		
		try {
			NetworkInterface updatedNwIf = null;
			if (secGroup == null) {
				logger.info("Removing security group from VM " + nwIf.virtualMachineId() + " and interface " + nwIf.id());
				updatedNwIf = nwIf.update().withoutNetworkSecurityGroup().apply();	
			} else {
				logger.info("Assigning security group " + secGroup.name() + " to VM " + nwIf.virtualMachineId() + " on interface " + nwIf.id());
				updatedNwIf = nwIf.update().withExistingNetworkSecurityGroup(secGroup).apply();	
			}
			
			logger.info("Successfully modified instance interface " + nwIf.id() + " from VM " + nwIf.virtualMachineId() 
				+ " on account " + accountName + ": " + jsonMapper.writeValueAsString(updatedNwIf));
			return true;
			
		} catch (Exception ex) {
			logger.error("Error when trying to assign or remove the security group for VM " + nwIf.virtualMachineId() + 
				" and interface " + nwIf.id() + " on account " + accountName, ex);
			return false;
		}
	}
	
	

	public boolean isEnableDebugRestClient() {
		return enableDebugRestClient;
	}


	public void setEnableDebugRestClient(boolean enableDebugRestClient) {
		this.enableDebugRestClient = enableDebugRestClient;
	}


}
