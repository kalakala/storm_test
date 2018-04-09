package com.my.test.test2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    Map<String,Integer> counts=new HashMap<String, Integer>();
    OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    public void execute(Tuple input) {
        String str=input.getStringByField("word");

        Integer count=counts.get(str);
        if(count==null){
            count=0;
        }
        count++;

        System.out.println("【WordCountBolt】thread name:"+Thread.currentThread().getName()+",word:"+str+",count:"+count);
        counts.put(str,count);
        collector.emit(new Values(str,count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }
}
