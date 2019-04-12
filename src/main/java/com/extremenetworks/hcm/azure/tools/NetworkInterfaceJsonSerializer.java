package com.extremenetworks.hcm.azure.tools;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.microsoft.azure.management.network.NetworkInterface;
import com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.microsoft.azure.management.network.NetworkSecurityRule;
import com.microsoft.azure.management.network.NicIPConfiguration;


public class NetworkInterfaceJsonSerializer extends JsonSerializer<NetworkInterface> {

	public void serialize(NetworkInterface nwInterface, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {

		jgen.writeStartObject();
		
		jgen.writeFieldName("id");
		jgen.writeString(nwInterface.id());
		
		jgen.writeFieldName("macAddress");
		jgen.writeString(nwInterface.macAddress());

		jgen.writeFieldName("name");
		jgen.writeString(nwInterface.name());

		jgen.writeFieldName("networkSecurityGroupId");
		jgen.writeString(nwInterface.networkSecurityGroupId());

		jgen.writeFieldName("primaryPrivateIP");
		jgen.writeString(nwInterface.primaryPrivateIP());

		jgen.writeFieldName("regionName");
		jgen.writeString(nwInterface.regionName());

		jgen.writeFieldName("resourceGroupName");
		jgen.writeString(nwInterface.resourceGroupName());
		
		jgen.writeFieldName("virtualMachineId");
		jgen.writeString(nwInterface.virtualMachineId());

		jgen.writeFieldName("internalDnsNameLabel");
		jgen.writeString(nwInterface.internalDnsNameLabel());

		jgen.writeFieldName("internalDomainNameSuffix");
		jgen.writeString(nwInterface.internalDomainNameSuffix());

		jgen.writeFieldName("internalFqdn");
		jgen.writeString(nwInterface.internalFqdn());

		jgen.writeFieldName("key");
		jgen.writeString(nwInterface.key());

		jgen.writeFieldName("update");
		jgen.writeString(nwInterface.update().toString());

		/* Print the network security group (including all rules) */
		printSecurityGroup(nwInterface.getNetworkSecurityGroup(), jgen);
		
		/* Print all IP configurations */
		printIpConfigurations(nwInterface.ipConfigurations(), jgen);
		
		
		/* Tags */
		jgen.writeArrayFieldStart("tags");
		
		Iterator<Entry<String, String>> itTags = nwInterface.tags().entrySet().iterator();
		while (itTags.hasNext()) {
			Entry<String, String> entryTag = itTags.next();

			jgen.writeStartObject();
			
			jgen.writeFieldName("key");
			jgen.writeString(entryTag.getKey());
			
			jgen.writeFieldName("value");
			jgen.writeString(entryTag.getValue());

			jgen.writeEndObject();
		}
		
		jgen.writeEndArray(); // End of: Tags 
		
		
		jgen.writeEndObject();
	}
	
	
	/** Prints the network security group */
	private void printSecurityGroup(NetworkSecurityGroup secGroup, JsonGenerator jgen) throws IOException, JsonProcessingException {
		
		if (secGroup == null) {
			jgen.writeFieldName("networkSecurityGroup");
			jgen.writeStartObject();
			jgen.writeEndObject();	
			return;
		}
		
		jgen.writeFieldName("networkSecurityGroup");
		jgen.writeStartObject();
		
		jgen.writeFieldName("id");
		jgen.writeString(secGroup.id());

		jgen.writeFieldName("key");
		jgen.writeString(secGroup.key());

		jgen.writeFieldName("name");
		jgen.writeString(secGroup.name());

		jgen.writeFieldName("regionName");
		jgen.writeString(secGroup.regionName());

		jgen.writeFieldName("resourceGroupName");
		jgen.writeString(secGroup.resourceGroupName());

		jgen.writeFieldName("type");
		jgen.writeString(secGroup.type());

		
		// Print all security rules
		printSecurityRules(secGroup, jgen);
		
		jgen.writeEndObject();	
	}
	
	
	/** Prints the list of security rules */
	private void printSecurityRules(NetworkSecurityGroup secGroup, JsonGenerator jgen) throws IOException, JsonProcessingException {
		
		jgen.writeArrayFieldStart("securityRules");
		
		Iterator<NetworkSecurityRule>itSecurityRules = secGroup.securityRules().values().iterator();
		
		while (itSecurityRules.hasNext()) {

			NetworkSecurityRule secRule = itSecurityRules.next();

			jgen.writeStartObject();
			
			jgen.writeFieldName("key");
			jgen.writeString(secRule.key());

			jgen.writeFieldName("name");
			jgen.writeString(secRule.name());
			
			jgen.writeFieldName("description");
			jgen.writeString(secRule.description());

			jgen.writeFieldName("destinationAddressPrefix");
			jgen.writeString(secRule.destinationAddressPrefix());

			jgen.writeFieldName("destinationPortRange");
			jgen.writeString(secRule.destinationPortRange());

			jgen.writeFieldName("sourceAddressPrefix");
			jgen.writeString(secRule.sourceAddressPrefix());

			jgen.writeFieldName("sourcePortRange");
			jgen.writeString(secRule.sourcePortRange());

			jgen.writeFieldName("priority");
			jgen.writeNumber(secRule.priority());

			jgen.writeFieldName("access");
			jgen.writeString(secRule.access().toString());

			jgen.writeFieldName("direction");
			jgen.writeString(secRule.direction().toString());

			jgen.writeFieldName("protocol");
			jgen.writeString(secRule.protocol().toString());

			/* Destination Application Security Group Ids */
			jgen.writeArrayFieldStart("destinationApplicationSecurityGroupIds");
			
			Iterator<String> itDestSecGroupIDs = secRule.destinationApplicationSecurityGroupIds().iterator();
			while (itDestSecGroupIDs.hasNext()) {
				jgen.writeString(itDestSecGroupIDs.next());
			}
			
			jgen.writeEndArray(); // End of: Destination Application Security Group Ids 
			

			/* Source Application Security Group Ids */
			jgen.writeArrayFieldStart("sourceApplicationSecurityGroupIds");
			
			Iterator<String> itSrcSecGroupIDs = secRule.sourceApplicationSecurityGroupIds().iterator();
			while (itSrcSecGroupIDs.hasNext()) {
				jgen.writeString(itSrcSecGroupIDs.next());
			}
			
			jgen.writeEndArray(); // End of: Source Application Security Group Ids
			
			
			jgen.writeEndObject(); // End of: Security Rule
		}
		
		jgen.writeEndArray(); // End of: Array of all security rules
	}
	
	
	/** Prints the network security group */
	private void printIpConfigurations(Map<String, NicIPConfiguration> ipConfigs, JsonGenerator jgen) throws IOException, JsonProcessingException {
		
		jgen.writeArrayFieldStart("IpConfigurations");
		
		Iterator<NicIPConfiguration> itIpConfigs = ipConfigs.values().iterator();
		
		while (itIpConfigs.hasNext()) {
			
			NicIPConfiguration ipConfig = itIpConfigs.next();

			jgen.writeStartObject();
			
			jgen.writeFieldName("key");
			jgen.writeString(ipConfig.key());

			jgen.writeFieldName("name");
			jgen.writeString(ipConfig.name());

			jgen.writeFieldName("networkId");
			jgen.writeString(ipConfig.networkId());

			if (ipConfig.getNetwork() != null) {
				jgen.writeFieldName("networkName");
				jgen.writeString(ipConfig.getNetwork().name());	
			}
			
			jgen.writeFieldName("privateIpAddress");
			jgen.writeString(ipConfig.privateIPAddress());

			jgen.writeFieldName("publicIpAddress");
			if (ipConfig.getPublicIPAddress() == null || ipConfig.getPublicIPAddress().ipAddress() == null) {
				jgen.writeString("");
			} else {
				jgen.writeString(ipConfig.getPublicIPAddress().ipAddress());	
			}
			

			jgen.writeFieldName("subnetName");
			jgen.writeString(ipConfig.subnetName());

			jgen.writeFieldName("isPrimary");
			jgen.writeBoolean(ipConfig.isPrimary());

			jgen.writeEndObject(); // End of: IP Config
		}

		jgen.writeEndArray(); // End of: Array of all IP configurations
	}
}