package cn.bigdata.storm.practice.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class MyCountBolt extends BaseRichBolt {

    OutputCollector collector;
    Map<String,Integer> map=new HashMap<String, Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;

    }

    @Override
    public void execute(Tuple input) {

        String word=input.getString(0);
        Integer num=input.getInteger(1);
        System.out.println(Thread.currentThread().getId()+"   word"+word);
        if(map.containsKey(word)){
            Integer count=map.get(word);
            map.put(word,count+num);
        }else{
            map.put(word,num);        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        //不输出
    }
}
