package com.my.test.test3;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class MySpout implements IRichSpout {
    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;

    SpoutOutputCollector spoutOutputCollector=null;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        String str=null;
        try {
            this.fis = new FileInputStream("F:\\data\\track.log");
            this.isr = new InputStreamReader(fis, "UTF-8");
            this.br = new BufferedReader(isr);
            this.spoutOutputCollector=collector;


            str=br.readLine();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    String str;

    public void nextTuple() {
        try {
            while ((str = this.br.readLine()) != null) {
                //过滤动作
                this.spoutOutputCollector.emit(new Values(str));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }




    public Map<String, Object> getComponentConfiguration() {


        return null;
    }
}
