package cn.bigdata.storm.kafka.simple;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ProducerDemo {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", "mini1:2181,mini2:2181,mini3:2181");
		props.put("metadata.broker.list", "mini1:9092,mini2:9092,mini3:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 1001; i <= 1100; i++)
			producer.send(new KeyedMessage<String, String>("test1", "it " + i));
	}
}