package com.extremenetworks.hcm.azure.tools;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.network.Subnet;


public class NetworkJsonSerializer extends JsonSerializer<Network> {

	public void serialize(Network nw, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {

		jgen.writeStartObject();
		
		jgen.writeFieldName("id");
		jgen.writeString(nw.id());

		jgen.writeFieldName("name");
		jgen.writeString(nw.name());

		jgen.writeFieldName("key");
		jgen.writeString(nw.key());

		jgen.writeFieldName("regionName");
		jgen.writeString(nw.regionName());

		jgen.writeFieldName("resourceGroupName");
		jgen.writeString(nw.resourceGroupName());

		
		printSubnets(nw.subnets(), jgen);
		
	
		/* Tags */
		jgen.writeArrayFieldStart("tags");
		
		Iterator<Entry<String, String>> itTags = nw.tags().entrySet().iterator();
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
	

	/** Prints the list of subnets */
	private void printSubnets(Map<String, Subnet> subnets, JsonGenerator jgen) throws IOException, JsonProcessingException {
		
		jgen.writeArrayFieldStart("subnets");
		
		Iterator<Subnet> itSubnets = subnets.values().iterator();
		
		while (itSubnets.hasNext()) {

			Subnet subnet = itSubnets.next();

			jgen.writeStartObject();
			
			jgen.writeFieldName("key");
			jgen.writeString(subnet.key());

			jgen.writeFieldName("name");
			jgen.writeString(subnet.name());
			
			jgen.writeFieldName("addressPrefix");
			jgen.writeString(subnet.addressPrefix());

			jgen.writeFieldName("networkSecurityGroupId");
			jgen.writeString(subnet.networkSecurityGroupId());

			jgen.writeFieldName("routeTableId");
			jgen.writeString(subnet.routeTableId());


			jgen.writeEndObject(); // End of: Subnet
		}
		
		jgen.writeEndArray(); // End of: Array of all subnets
	}
	
}