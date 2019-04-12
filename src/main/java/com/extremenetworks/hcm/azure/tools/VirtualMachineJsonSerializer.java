package com.extremenetworks.hcm.azure.tools;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.resources.fluentcore.arm.AvailabilityZoneId;


public class VirtualMachineJsonSerializer extends JsonSerializer<VirtualMachine> {

	public void serialize(VirtualMachine vm, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {

		jgen.writeStartObject();
		
		jgen.writeFieldName("id");
		jgen.writeString(vm.id());

		jgen.writeFieldName("vmId");
		jgen.writeString(vm.vmId());

		jgen.writeFieldName("name");
		jgen.writeString(vm.name());

		jgen.writeFieldName("key");
		jgen.writeString(vm.key());

		jgen.writeFieldName("availabilitySetId");
		jgen.writeString(vm.availabilitySetId());

		jgen.writeFieldName("computerName");
		jgen.writeString(vm.computerName());

		jgen.writeFieldName("primaryNetworkInterfaceId");
		jgen.writeString(vm.primaryNetworkInterfaceId());

		jgen.writeFieldName("provisioningState");
		jgen.writeString(vm.provisioningState());

		jgen.writeFieldName("powerState");
		jgen.writeString(vm.powerState().toString());
		
		jgen.writeFieldName("regionName");
		jgen.writeString(vm.regionName());

		jgen.writeFieldName("resourceGroupName");
		jgen.writeString(vm.resourceGroupName());

		jgen.writeFieldName("size");
		jgen.writeString(vm.size().toString());

		if (vm.getPrimaryPublicIPAddress() != null) {
			jgen.writeFieldName("primaryPublicIpAddress");
			jgen.writeString(vm.getPrimaryPublicIPAddress().ipAddress());	
		} else {
			jgen.writeFieldName("primaryPublicIpAddress");
			jgen.writeString("");
		}

		jgen.writeFieldName("deviceFamily");
		jgen.writeString(vm.storageProfile().osDisk().osType().toString());

		/* The info on a more exact operating system version is not available for custom VM images */
		if (vm.storageProfile().imageReference() != null) {
			
			jgen.writeFieldName("deviceType");
			jgen.writeString(vm.storageProfile().imageReference().offer() + " " + vm.storageProfile().imageReference().sku());
		} 


		/* Availability Zones */
		jgen.writeArrayFieldStart("availabilityZones");
		
		Iterator<AvailabilityZoneId> itZones = vm.availabilityZones().iterator();
		while (itZones.hasNext()) {
			jgen.writeString(itZones.next().toString());			
		}
		
		jgen.writeEndArray(); // End of: Availability Zones 
		

		/* Network Interface IDs */
		jgen.writeArrayFieldStart("networkInterfaceIDs");
		
		Iterator<String> itnwIfIDs = vm.networkInterfaceIds().iterator();
		while (itnwIfIDs.hasNext()) {
			jgen.writeString(itnwIfIDs.next());			
		}
		
		jgen.writeEndArray(); // End of: Network Interface IDs 
		
	
		/* Tags */
		jgen.writeArrayFieldStart("tags");
		
		Iterator<Entry<String, String>> itTags = vm.tags().entrySet().iterator();
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
}