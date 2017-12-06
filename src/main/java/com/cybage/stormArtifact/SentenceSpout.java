package com.cybage.stormArtifact;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;



public class SentenceSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String[] sentences = {
			"Hello This is Storm WordCount program",
			"I am newbie to Storm",
			"WordCount is a simple stream example ",
			"SentenceSpout class will simply emit a stream of single value tuples",
			"we are running Storm in local mode using Storm Local Cluster class",
			"Hello Shraddha"
	};
	private int index = 0;
	//This method declares that SentenceSpout is going to emit line tuple
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	//This method would get called at the start and will give you context information.
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}
	//This method passes one tuple to storm for processing at a time, in this method i am just reading one line from sentence array
	//and pass it to tuple
	public void nextTuple() {
		Utils.sleep(1000);
		this.collector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		
	}
	
}
