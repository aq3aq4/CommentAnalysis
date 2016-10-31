package cwh.posting.analysis.bolt.email;

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

public class EmailCountBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;
	private Map<String, BigDecimal> emailCountMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		emailCountMap = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String email = tuple.getStringByField("email");
		BigDecimal count = emailCountMap.get(email);
		
		if(count == null) {
			count = new BigDecimal(0);
		}
		
		count = count.add(new BigDecimal(1));
		emailCountMap.put(email, count);
		
		this.collector.emit(new Values(emailCountMap));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("emailCountMap"));
	}
	
}
