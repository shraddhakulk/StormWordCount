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
	private String[] sentences = { "Hello This is Storm WordCount program", "I am newbie to Storm",
			"WordCount is a simple stream example ",
			"SentenceSpout class will simply emit a stream of single value tuples",
			"we are running Storm in local mode using Storm Local Cluster class", "Hello Shraddha" };
	private int index = 0;

	// This method declares that SentenceSpout is going to emit line tuple
	// used to tell Storm what streams a component will emit and the fields each
	// stream's tuples will contain.
	// In this case, we're declaring that our spout will emit a single
	// stream of tuples containing a single field ("sentence").
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	// Open is called whenever spout component is initialized.
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	//Storm calls this method to request that the spout emit tuples to the output collector.
	// This method passes one tuple to storm for processing at a time
	// Below method is just reading one line from sentence array
	// and passing it to tuple
	public void nextTuple() {
		Utils.sleep(1000);
		this.collector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}

	}

}
