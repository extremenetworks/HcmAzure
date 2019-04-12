package com.extremenetworks.hcm.azure.tools;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.microsoft.azure.management.network.NetworkSecurityRule;


public class NetworkSecurityGroupJsonSerializer extends JsonSerializer<NetworkSecurityGroup> {

	public void serialize(NetworkSecurityGroup secGroup, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {

		jgen.writeStartObject();
		
		jgen.writeFieldName("id");
		jgen.writeString(secGroup.id());

		jgen.writeFieldName("name");
		jgen.writeString(secGroup.name());

		jgen.writeFieldName("key");
		jgen.writeString(secGroup.key());

		jgen.writeFieldName("regionName");
		jgen.writeString(secGroup.regionName());

		jgen.writeFieldName("resourceGroupName");
		jgen.writeString(secGroup.resourceGroupName());

		
		
		printSecurityRules(secGroup.defaultSecurityRules(), true, jgen);
		
		printSecurityRules(secGroup.securityRules(), false, jgen);
		
	
		/* Tags */
		jgen.writeArrayFieldStart("tags");
		
		Iterator<Entry<String, String>> itTags = secGroup.tags().entrySet().iterator();
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
	

	/** Prints the list of default security rules */
	private void printSecurityRules(Map<String, NetworkSecurityRule> secRules, boolean areDefaultRules, JsonGenerator jgen) throws IOException, JsonProcessingException {
		
		if (areDefaultRules) {
			jgen.writeArrayFieldStart("defaultSecurityRules");	
		} else {
			jgen.writeArrayFieldStart("securityRules");
		}
		
		
		Iterator<NetworkSecurityRule> itSecurityRules = secRules.values().iterator();
		
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
	
}