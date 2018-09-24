package cn.bigdata.storm.kafka.simple;


import kafka.producer.Partitioner;

public class MyLogPartitioner implements Partitioner {
    @Override
    public int partition(Object o, int i) {
        return 0;
    }

    //private static Logger
}
