package cn.bigdata.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

public class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector collector;

    //初始化方法
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector=spoutOutputCollector;
    }

    //storm 框架在while(true)中 调用nextTuple方法
    public void nextTuple() {
        collector.emit(new Values("i am lilei love hanmeimei"));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("love"));
    }
}
