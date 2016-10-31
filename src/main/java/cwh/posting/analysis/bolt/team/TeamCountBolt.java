package cwh.posting.analysis.bolt.team;

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

public class TeamCountBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector = null;
	private Map<String, BigDecimal> teamCountMap= null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		teamCountMap = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String team = tuple.getStringByField("team");
		BigDecimal count = teamCountMap.get(team);
		
		if(count == null) {
			count = new BigDecimal(0);
		}
		
		
		count = count.add(new BigDecimal(1));
		teamCountMap.put(team, count);
		
		collector.emit(new Values(teamCountMap));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("teamCountMap"));
	}
	
}
