package cn.bigdata.storm.ackfail;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class MySpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random rand;
    private Map<String,Values> buffer=new HashMap<>();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        System.out.println("1");

        declarer.declare(new Fields("sentence"));
        rand=new Random();

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        System.out.println("2");
    }

    @Override
    public void nextTuple() {

        String[] sentences=new String[]{"the cow jumped over the moon",
                 "the cow jumped over the moon",
                 "the cow jumped over teh moon",
                "the cow jumped over teh moon","the cow jumped over teh moon"};
        String sentence=sentences[rand.nextInt(sentences.length)];
        String messageId=UUID.randomUUID().toString().replace("-","");
        Values tuple=new Values(sentence);
        collector.emit(tuple,messageId);
        buffer.put(messageId,tuple);
        System.out.println("3");

        try {
            Thread.sleep(5000);
        }catch (InterruptedException e){
            e.printStackTrace();

        }


    }

    @Override
    public void ack(Object msgId) {
        System.out.println("消息处理成功，id="+msgId);
        buffer.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("消息处理失败，id="+msgId);
        Values tuple=buffer.get(msgId);
        collector.emit(tuple,msgId);
    }
}
