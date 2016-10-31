package cwh.posting.analysis.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import cwh.posting.analysis.topology.CommentAnalyTopology;

/**
 * 실시간으로 집계되는 결과를 받아서 스트림을 흘리지 않고 뷰나 디비로 처리 할 수 있도록 구성될 Bolt
 * @author birdhead
 */
public class TotalGetterBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private Map<String, Double> totalMap = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.totalMap = new HashMap<>();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		String sourceComponent = tuple.getSourceComponent();
		
		if(sourceComponent.equals(CommentAnalyTopology.GENDERRATECALC_BOLT)) {
			Map<String, Double> genderRateMap = (Map<String, Double>) tuple.getValueByField("genderRateMap");
			System.err.println(genderRateMap.toString());
		} else if(sourceComponent.equals(CommentAnalyTopology.AGERATECALC_BOLT)) {
			Map<String, Double> ageRateMap = (Map<String, Double>) tuple.getValueByField("ageRateMap");
			System.err.println(ageRateMap.toString());
		} else if(sourceComponent.equals(CommentAnalyTopology.TEAMRATECALC_BOLT)) {
			Map<String, Double> teamRateMap = (Map<String, Double>) tuple.getValueByField("teamRateMap");
			System.err.println(teamRateMap.toString());
		} else if(sourceComponent.equals(CommentAnalyTopology.EMAILRATECALC_BOLT)) {
			Map<String, Double> emailRateMap = (Map<String, Double>)tuple.getValueByField("emailRateMap");
			System.err.println(emailRateMap.toString());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	
}
