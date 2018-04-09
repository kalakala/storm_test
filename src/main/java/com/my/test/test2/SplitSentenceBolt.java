package com.my.test.test2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBolt implements IRichBolt {
    private OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    public void execute(Tuple input) {
        String sentence=input.getStringByField("word");
        String [] strings=sentence.split(" ");

        System.out.println("【SplitSentenceBolt】thread name:"+Thread.currentThread().getName()+"，收到的词："+sentence);
        for(String word:strings){
            this.collector.emit(new Values(word));
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
