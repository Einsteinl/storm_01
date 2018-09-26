package cn.bigdata.storm.kafkawithstorm.order;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class OrderMaSender {

    public static void main(String[] args) {
        String TOPIC="orderMQ";
        Properties props=new Properties();
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","mini1:9092,mini2:9092,mini3:9092");
        props.put("request.required.acks","1");
        props.put("partitioner.class","kafka.producer.DefaultPartitioner");

        Producer<String,String> producer=new Producer<String, String>(new ProducerConfig(props));
        for (int messageNo=1;messageNo<100000;messageNo++){
            producer.send(new KeyedMessage<String, String>(TOPIC,messageNo+"",new OrderInfo().random(String.valueOf(messageNo))));
            try {
                Thread.sleep(100);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
