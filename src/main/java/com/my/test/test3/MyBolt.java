package com.my.test.test3;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class MyBolt implements IRichBolt {
    OutputCollector collector=null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector=outputCollector;
    }
    int num=0;
    String value=null;
    public void execute(Tuple tuple) {
        try{
            value=tuple.getStringByField("log");
            if(!StringUtils.isEmpty(value)){
                num++;
                System.err.println(Thread.currentThread().getName()+",num:"+num+",session_id:"+value.split("\t")[1]);
            }
            collector.ack(tuple);
        }catch (Exception ex){
            collector.fail(tuple);
            ex.printStackTrace();
        }

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
