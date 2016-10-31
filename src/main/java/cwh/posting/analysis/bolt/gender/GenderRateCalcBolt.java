package cwh.posting.analysis.bolt.gender;

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

public class GenderRateCalcBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector = null;
	private Map<String, BigDecimal> genderRateMap = null;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.genderRateMap = new HashMap<>();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
		Map<String, BigDecimal> genderCountMap = (Map<String, BigDecimal>)tuple.getValueByField("genderCountMap");
		
		BigDecimal totalCount = new BigDecimal(0);
		
		Set<String> keySets = genderCountMap.keySet();
		
		for(String key : keySets) {
			totalCount = totalCount.add(genderCountMap.get(key));
		}
		
		for(String key : keySets) {
			BigDecimal genderCount = genderCountMap.get(key);
			BigDecimal genderRate = genderCount.divide(totalCount, 3, BigDecimal.ROUND_HALF_UP);
			
			genderRateMap.put(key, genderRate);			
		}
		
		this.collector.emit(new Values(genderRateMap));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("genderRateMap"));
	}
	
}
