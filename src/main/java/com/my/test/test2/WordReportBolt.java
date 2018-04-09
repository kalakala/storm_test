package com.my.test.test2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class WordReportBolt extends BaseRichBolt {
    Map<String,Integer> counts=new HashMap<String, Integer>();
    private OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    public void execute(Tuple input) {
        Integer count=input.getIntegerByField("count");
        String word=input.getStringByField("word");

        System.out.println("【WordReportBolt】Thread name:"+Thread.currentThread().getName()+",word:"+word+",count:"+count);
        this.counts.put(word,count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("-----------------FINAL COUNTS  START-----------------------");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);

        for(String key : keys){
            System.out.println(key + " : " + this.counts.get(key));
        }

        System.out.println("-----------------FINAL COUNTS  END-----------------------");
    }
}
