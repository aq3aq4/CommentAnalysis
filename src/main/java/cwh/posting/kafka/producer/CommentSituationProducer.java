package cwh.posting.kafka.producer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import cwh.posting.kafka.domain.PeopleDomain;
import cwh.posting.kafka.domain.PeopleDomainSerializer;
import cwh.posting.utils.DataSetUtils;

public class CommentSituationProducer implements Runnable{

	private Properties props = new Properties();
	
	public CommentSituationProducer() {
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", PeopleDomainSerializer.class.getName());
	}

	@Override
	public void run() {
		Random random = new Random();
		
		while(true) {
			Producer<String, PeopleDomain> producer = new KafkaProducer<>(props);
			PeopleDomain domain = new PeopleDomain();
			
			domain.setGender(DataSetUtils.genderMap.get(random.nextInt(2)));
			domain.setTeam(DataSetUtils.teamMap.get(random.nextInt(10)));
			domain.setAge(random.nextInt(50 - 10 + 1) + 10);
			domain.setEmail(DataSetUtils.emailMap.get(random.nextInt(3)));
			
			producer.send(new ProducerRecord<String, PeopleDomain>("userInfo", domain));
			producer.close();
			
			try {
				Thread.sleep(1000);
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
