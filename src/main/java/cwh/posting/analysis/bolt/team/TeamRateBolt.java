package cwh.posting.analysis.bolt.team;

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

public class TeamRateBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector = null;
	private Map<String, Object> teamRateMap = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.teamRateMap = new HashMap<>();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
		Map<String, BigDecimal> teamCountMap = (Map<String, BigDecimal>)tuple.getValueByField("teamCountMap");
		
		BigDecimal totalCount = new BigDecimal(0);
		
		Set<String> keySets = teamCountMap.keySet();
		
		for(String key : keySets) {
			totalCount = totalCount.add(teamCountMap.get(key));
		}
		
		for(String key : keySets) {
			BigDecimal teamCount = teamCountMap.get(key);
			BigDecimal teamRate = teamCount.divide(totalCount, 3, BigDecimal.ROUND_HALF_UP);
			
			teamRateMap.put(key, teamRate);
		}
		
		this.collector.emit(new Values(teamRateMap));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("teamRateMap"));
	}

}
