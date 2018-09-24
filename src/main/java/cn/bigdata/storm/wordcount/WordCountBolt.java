package cn.bigdata.storm.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

    OutputCollector collector;
    Map<String,Integer> map=new HashMap<String, Integer>();


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    public void execute(Tuple input) {

        String word=input.getString(0);
        Integer num=input.getInteger(1);
        System.out.println(Thread.currentThread().getId()+"  word:"+word);
        if(map.containsKey(word)){
            Integer count=map.get(word);
            map.put(word,count+num);
        }else{
            map.put(word,num);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        //不输出
    }
}
