package com.cybage.stormArtifact;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	

	//This is where you would prepare resources such as database connections during bolt initialization. 
	//Here method simply saves a reference to the OutputCollector object.
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	//Execute method is called every time the bolt receives a tuple from a stream to which it subscribes. 
	//In this case, it looks up the value of the "sentence" field of the incoming tuple as a string, splits the value into individual words, and emits a new tuple for each word.
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words){
			this.collector.emit(new Values(word));
		}
	}
	 
	//It declares class will emit a single stream of tuples, each containing one field ("word").
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}