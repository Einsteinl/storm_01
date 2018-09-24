package cn.bigdata.storm.kafka.simple;


import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;


public class MyLogPartitioner implements Partitioner {

    private static Logger logger= Logger.getLogger(MyLogPartitioner.class);

    public  MyLogPartitioner(VerifiableProperties ){

    }

    @Override
    public int partition(Object obj, int numPartitions) {
        return Integer.parseInt(obj.toString())%numPartitions;
    }

    //private static Logger
}
