package cn.bigdata.storm.kafkawithstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import cn.bigdata.storm.kafkawithstorm.order.ParserOrderPracticeBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaAndStormTopologyMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout(
                "kafkaSpout",
                new KafkaSpout(new SpoutConfig(new ZkHosts("mini1:2181,mini2:2181,mini3:2181"),
                        "orderMQ",
                        "/myKafka",
                        "kafkaSpout")),1);
        topologyBuilder.setBolt("mybolt1",new ParserOrderPracticeBolt(),1).shuffleGrouping("kafkaSpout");

        Config config=new Config();
        config.setNumWorkers(1);

        //提交任务 ---两种模式 本地模式和集群模式
        if(args.length>0){
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else {
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("storm2kafka",config,topologyBuilder.createTopology());
        }
    }
}
