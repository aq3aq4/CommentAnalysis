package cwh.posting.kafka.domain;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PeopleDomainSerializer implements Serializer<PeopleDomain> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public byte[] serialize(String topic, PeopleDomain data) {
		byte[] byteArray = null;
		
		ObjectMapper mapper = new ObjectMapper();
		
		try{
			byteArray = mapper.writeValueAsBytes(data);
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return byteArray;
	}

	@Override
	public void close() {
		
	}
	
}
