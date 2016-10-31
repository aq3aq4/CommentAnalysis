package cwh.posting.kafka.domain;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PeopleDomainDeserializer implements Deserializer<PeopleDomain> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public PeopleDomain deserialize(String topic, byte[] data) {
		PeopleDomain domain = null;
		ObjectMapper mapper = new ObjectMapper();

		try {
			domain = mapper.readValue(data, PeopleDomain.class);
			return domain;
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void close() {
		
	}
	
}
