package cn.bigdata.storm.practice.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class MySpout extends BaseRichSpout {

    SpoutOutputCollector collector;


    //初始化方法
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector=collector;
    }

    //storm 框架在while（true）调用nextTuple
    @Override
    public void nextTuple() {

        collector.emit(new Values("i am lilei love hanmeimei"));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {


        declarer.declare(new Fields("love"));
    }
}
