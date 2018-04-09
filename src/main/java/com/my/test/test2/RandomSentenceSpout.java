package com.my.test.test2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    Random random=null;
    String[] sentences = new String[]{
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"};

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
        this.random=new Random();
    }
    public void nextTuple() {
        Utils.sleep(100);

      //  final String sentence=sentences[this.random.nextInt(sentences.length)];
        for(int j=0;j<3;j++) {
            for (int count=0;count < sentences.length; count++) {
                String sentence = sentences[count];
                System.err.println("【RandomSentenceSpout】thread name:" + Thread.currentThread().getName() + ",发射的单词：" + sentence + ",count:" + count);
                this.collector.emit(new Values(sentence));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 5);

        //ShuffleGrouping：随机选择一个Task来发送。
        builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
        //FiledGrouping：根据Tuple中Fields来做一致性hash，相同hash值的Tuple被发送到相同的Task。
        builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));
        //GlobalGrouping：所有的Tuple会被发送到某个Bolt中的id最小的那个Task。
        builder.setBolt("report", new WordReportBolt(), 6).globalGrouping("count");

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(20000);

            cluster.shutdown();
        }
    }
}
