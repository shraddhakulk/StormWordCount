package com.cybage.stormArtifact;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	public static void main(String[] args) throws Exception {
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		
		TopologyBuilder builder = new TopologyBuilder();
		
		//defining new spout to the topology 
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		
		//defining new bolt to the topology 
		//ID is the id of this component. This id is referenced by other components that want to consume this bolt's outputs
		builder.setBolt(SPLIT_BOLT_ID, splitBolt)
		.shuffleGrouping(SENTENCE_SPOUT_ID);
		

		builder.setBolt(COUNT_BOLT_ID, countBolt)
		.fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		
		builder.setBolt(REPORT_BOLT_ID, reportBolt)
		.globalGrouping(COUNT_BOLT_ID);
		
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Utils.sleep(20000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}
}
