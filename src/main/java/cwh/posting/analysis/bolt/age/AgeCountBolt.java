package cwh.posting.analysis.bolt.age;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AgeCountBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector = null;
	private Map<String, BigDecimal> ageCountMap = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.ageCountMap = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		int age = tuple.getIntegerByField("age");
		String key = distAgeNMakedkey(age);
		BigDecimal count = ageCountMap.get(key);
		
		if(count == null) {
			count = new BigDecimal(0);
		}
		
		count = count.add(new BigDecimal(1));
		ageCountMap.put(key, count);
		
		collector.emit(new Values(ageCountMap));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ageCountMap"));
	}
	
	public String distAgeNMakedkey(int age) {
		String key = "";
		
		if(age >= 10 && age < 20) {
			key = "10대";
		} else if(age >= 20 && age < 30) {
			key = "20대";
		} else if(age >= 30 && age < 40) {
			key = "30대";
		} else if(age >= 40 && age < 50) {
			key = "40대";
		} else if(age >= 50 && age < 60) {
			key = "50대";
		} else {
			key = "50대이상";
		}
		
		return key;
	}
}
