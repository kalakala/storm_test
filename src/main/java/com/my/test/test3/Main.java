package com.my.test.test3;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class Main {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        TopologyBuilder builder =new TopologyBuilder();
        builder.setSpout("mySpout",new MySpout(),2);
        builder.setBolt("myBolt",new MyBolt(),1).shuffleGrouping("mySpout");

        Config config=new Config();
        config.setDebug(false);
        config.setNumWorkers(4);

        LocalCluster localCluster=new LocalCluster();
        if(args.length>0){
            StormSubmitter.submitTopology("mytopology",config,builder.createTopology());
        }else {
            localCluster.submitTopology("mytopology", config, builder.createTopology());
        }

    }
}
