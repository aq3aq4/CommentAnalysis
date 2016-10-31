package cwh.posting.analysis.bolt.email;

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

public class EmailRateBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector = null;
	private Map<String, BigDecimal> emailRateMap = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.emailRateMap = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		Map<String, BigDecimal> emailCountMap = (Map<String, BigDecimal>) tuple.getValueByField("emailCountMap");
		
		BigDecimal totalCount = new BigDecimal(0);
		
		Set<String> keySets = emailCountMap.keySet();
		
		for(String key : keySets) {
			totalCount = totalCount.add(emailCountMap.get(key));
		}
		
		for(String key : keySets) {
			BigDecimal emailCount = emailCountMap.get(key);
			BigDecimal emailRate = emailCount.divide(totalCount, 3, BigDecimal.ROUND_HALF_UP);
			
			emailRateMap.put(key, emailRate);
		}
		
		this.collector.emit(new Values(emailRateMap));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("emailRateMap"));
	}

}
