package cwh.posting.analysis.bolt.age;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AgeRateCalcBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector = null;
	private Map<String, BigDecimal> ageRateMap = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.ageRateMap = new HashMap<>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		Map<String, BigDecimal> ageCountMap = (Map<String, BigDecimal>)tuple.getValueByField("ageCountMap");
		
		BigDecimal totalCount = new BigDecimal(0);
		
		Set<String> keySets = ageCountMap.keySet();
		
		for(String key : keySets) {
			totalCount = totalCount.add(ageCountMap.get(key));
		}
		
		for(String key : keySets) {
			BigDecimal ageCount = ageCountMap.get(key);
			BigDecimal ageRate = ageCount.divide(totalCount, 3, BigDecimal.ROUND_HALF_UP);
			
			ageRateMap.put(key, ageRate);
		}
		
		this.collector.emit(new Values(ageRateMap));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ageRateMap"));
	}

}
