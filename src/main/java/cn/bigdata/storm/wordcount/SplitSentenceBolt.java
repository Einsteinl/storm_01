package cn.bigdata.storm.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt {

    OutputCollector collector;

    //初始化方法
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
    }

    //被storm框架 while(true) 循环调用 传入参数tuple
    public void execute(Tuple input) {

        String line=input.getString(0);
        String[] arrWords=line.split(" ");
        for (String word:arrWords
             ) {
            collector.emit(new Values(word,1));

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word","num"));
    }
}
