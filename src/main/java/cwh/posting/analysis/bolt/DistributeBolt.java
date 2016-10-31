package cwh.posting.analysis.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;

import cwh.posting.kafka.domain.PeopleDomain;

public class DistributeBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collctor;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("gender", "team", "email", "age"));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collctor = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		ObjectMapper mapper = new ObjectMapper();
		String tupleValue = (tuple.getStringByField("str"));
		PeopleDomain domain = null;
		
		try {
			domain = mapper.readValue(tupleValue, PeopleDomain.class);
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		String gender = domain.getGender();
		String team = domain.getTeam();
		String email = domain.getEmail();
		int age = domain.getAge();
		
		collctor.emit(new Values(gender, team, email, age));
		
	}
	
}
