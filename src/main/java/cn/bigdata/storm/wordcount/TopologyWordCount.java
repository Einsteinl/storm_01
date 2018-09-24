package cn.bigdata.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyWordCount {

    public static void main(String args[]) throws Exception {

//1、准备一个TopologyBuilder

        //storm框架支持多语言，在java环境下创建一个拓扑，需要使用TopologyBuilder进行构建
        TopologyBuilder builder=new TopologyBuilder();

        //RandomSentenceSpout类，在已知的英文句子中，随机发送一条句子出去
        builder.setSpout("spout",new RandomSentenceSpout(),2);

        //SplicSentenceBolt类，主要是将一行一行的内容切割成单词
        builder.setBolt("split",new SplitSentenceBolt(),2).shuffleGrouping("spout");

        //WordCountBolt类，负责对单词的频率进行累加
        builder.setBolt("count",new WordCountBolt(),4).fieldsGrouping("split",new Fields("word"));

//2、创建一个configuration，用来指定当前topology 需要的worker的数量
        //启动topology的配置信息
        Config conf=new Config();
        //TOPOLOGY_DEBUG(setDebug),当它被设置成true的话，storm会记录下每个组件所发射的每条信息。
        //这在本地环境调试topology很有用，但是在线上这么做的话会影响性能的
        //conf.setDebug(true);

        //定义你希望集群分配多少个工作进程给你来执行这个topology
        conf.setNumWorkers(3);

//3、提交任务---两种模式  本地模式和集群模式

//        LocalCluster localCluster=new LocalCluster();
//        localCluster.submitTopology("mywordcount",conf,builder.createTopology());


        //向集群提交topology
        StormSubmitter.submitTopology("mywordcount",conf,builder.createTopology());
    }



}
