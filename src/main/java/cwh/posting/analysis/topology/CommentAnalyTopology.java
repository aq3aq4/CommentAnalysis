package cwh.posting.analysis.topology;

import java.util.UUID;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.ImmutableList;

import cwh.posting.analysis.bolt.TotalGetterBolt;
import cwh.posting.analysis.bolt.age.AgeCountBolt;
import cwh.posting.analysis.bolt.age.AgeRateCalcBolt;
import cwh.posting.analysis.bolt.email.EmailCountBolt;
import cwh.posting.analysis.bolt.email.EmailRateBolt;
import cwh.posting.analysis.bolt.gender.GenderCountBolt;
import cwh.posting.analysis.bolt.gender.GenderRateCalcBolt;
import cwh.posting.analysis.bolt.team.TeamCountBolt;
import cwh.posting.analysis.bolt.team.TeamRateBolt;
import cwh.posting.analysis.bolt.DistributeBolt;
import cwh.posting.kafka.producer.CommentSituationProducer;

public class CommentAnalyTopology {
	public static final String zkHost = "localhost:2181";
	public static final String SPOUT = "spout";
	public static final String DISTRITUBE_BOLT = "distributeBolt";
	public static final String GENDERCOUNT_BOLT = "genderCountBolt";
	public static final String AGECOUNT_BOLT = "ageCountBolt";
	public static final String TEAMCOUNT_BOLT = "teamCountBolt";
	public static final String EMAILCOUNT_BOLT = "emailCountBolt";
	
	public static final String GENDERRATECALC_BOLT = "genderRateCalcBolt";
	public static final String AGERATECALC_BOLT = "ageRateCalcBolt";
	public static final String EMAILRATECALC_BOLT = "emialRateCalcBolt";
	public static final String TEAMRATECALC_BOLT = "teamRateCalcBolt";
	
	public static final String TOTALGETTER_BOLT = "totalGetterBolt";
	
	public static void main(String[] args) throws InterruptedException {
		CommentSituationProducer producer = new CommentSituationProducer();
		Thread thread = new Thread(producer);
		thread.start();
		
		Thread.sleep(3000);
		ZkHosts zkHosts = new ZkHosts(zkHost);
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "userInfo", "/userInfo", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//		spoutConfig.zkServers = ImmutableList.of("localhost");
//		spoutConfig.zkPort = 2181;
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SPOUT, new KafkaSpout(spoutConfig), 1);
		builder.setBolt(DISTRITUBE_BOLT, new DistributeBolt()).shuffleGrouping(SPOUT);
		builder.setBolt(GENDERCOUNT_BOLT, new GenderCountBolt()).fieldsGrouping(DISTRITUBE_BOLT, new Fields("gender"));
		builder.setBolt(AGECOUNT_BOLT, new AgeCountBolt()).shuffleGrouping(DISTRITUBE_BOLT);
		builder.setBolt(TEAMCOUNT_BOLT, new TeamCountBolt()).shuffleGrouping(DISTRITUBE_BOLT);
		builder.setBolt(EMAILCOUNT_BOLT, new EmailCountBolt()).shuffleGrouping(DISTRITUBE_BOLT);
		
		builder.setBolt(GENDERRATECALC_BOLT, new GenderRateCalcBolt()).shuffleGrouping(GENDERCOUNT_BOLT);
		builder.setBolt(AGERATECALC_BOLT, new AgeRateCalcBolt()).shuffleGrouping(AGECOUNT_BOLT);
		builder.setBolt(TEAMRATECALC_BOLT, new TeamRateBolt()).shuffleGrouping(TEAMCOUNT_BOLT);
		builder.setBolt(EMAILRATECALC_BOLT, new EmailRateBolt()).shuffleGrouping(EMAILCOUNT_BOLT);
		
		builder.setBolt(TOTALGETTER_BOLT, new TotalGetterBolt())
				.globalGrouping(GENDERRATECALC_BOLT).globalGrouping(AGERATECALC_BOLT)
				.globalGrouping(TEAMRATECALC_BOLT).globalGrouping(EMAILRATECALC_BOLT);
				
		Config config = new Config();
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("topology", config, builder.createTopology());
		
//		Thread.sleep(10000);
//		cluster.shutdown();
	}
}




//TridentTopology topology = new TridentTopology();
//ZkHosts zk = new ZkHosts("localhost:2181");
//TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "userInfo");
//spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//
//TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(spoutConf);
//
//Stream eventStream = topology.newStream("log", kafkaSpout);
//
//StormTopology stormTopology = topology.build();
//
//LocalCluster cluster = new LocalCluster();
//
//Config config = new Config();
//
//cluster.submitTopology("kafkaTopology", config, stormTopology);
//
//try{
//	Thread.sleep(10000);
//}catch(Exception e) {
//	e.printStackTrace();
//}
//
//cluster.shutdown();