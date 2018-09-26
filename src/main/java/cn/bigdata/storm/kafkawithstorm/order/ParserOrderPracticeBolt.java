package cn.bigdata.storm.kafkawithstorm.order;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.naming.Name;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ParserOrderPracticeBolt extends BaseRichBolt {

    private JedisPool pool;
    //家电Map
    private Map<String,String> appMap;
    //手机Map
    private Map<String,String> phoneMap;
    //男装Map
    private Map<String,String> manMap;
    //美妆Map
    private Map<String,String> beautyMap;
    //汽车
    private Map<String,String> automobileMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //change "maxActive" -> "maxTotal" and "maxWait" -> "maxWaitMills" in all examples
        JedisPoolConfig config=new JedisPoolConfig();

        //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(5);

        //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取：
        //如果赋值为-1，则表示不限制：如果pool已经分配了maxActive个jedis实例
        //，则此时pool的状态为exhausted(耗尽)。
        //在borrow一个jedis实例时，是否提前进行validate操作：如果为true，则得到的jedis实例均是可用的。
        config.setMaxTotal(1000*100);

        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException
        config.setMaxWaitMillis(30);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        /**
         * 如果你遇到 java.net.SocketTimeoutException: Read timed out exception 的异常信息
         * 请尝试在构造JedisPool的时候设置自己的超时值，JedisPool默认的超时时间是2秒(单位毫秒)
         */
        pool=new JedisPool(config,"mini1",6379);

        //初始化Map
        appMap=new HashMap<String,String>();
        phoneMap=new HashMap<String,String>();
        manMap=new HashMap<String,String>();
        beautyMap=new HashMap<String,String>();
        automobileMap=new HashMap<String,String>();
        appMap.put("c","家电");
        appMap.put("s","小涛店铺");
        appMap.put("p","苏泊尔");

        phoneMap.put("c","手机");
        phoneMap.put("s","天天网购");
        phoneMap.put("p","小米");

        manMap.put("c","男装");
        manMap.put("s","丽人雅阁");
        manMap.put("p","花花公子");

        beautyMap.put("c","美妆");
        beautyMap.put("s","女人家");
        beautyMap.put("p","雅诗兰黛");

        automobileMap.put("c","汽车");
        automobileMap.put("s","老车城");
        automobileMap.put("p","兰德酷路泽");
    }

    @Override
    public void execute(Tuple tuple) {



        Jedis jedis=pool.getResource();
        //获取kafkaSpout发送过来的数据，是一个json
        String string=new String((byte[]) tuple.getValue(0));

        //解析json
        OrderInfo orderInfo=(OrderInfo) new Gson().fromJson(string,OrderInfo.class);

        //整个网站，各个业务线，各个品类，各个店铺，各个品牌，每个商品
        //获取整个网站的金额统计指标
        //String totalAmount=jedis.get("totalAmount");
        jedis.incrBy("totalAmount",orderInfo.getProductPrice());

        //获取商品所属业务线的指标信息
        String cid=getBubyProductId(orderInfo.getProductId(),"c");
        String sid=getBubyProductId(orderInfo.getProductId(),"s");
        String pid=getBubyProductId(orderInfo.getProductId(),"p");

        //对商品所属的品类进行累加
        jedis.zincrby("c", orderInfo.getProductPrice(), cid);
        //对商品所属的店铺进行累加
        jedis.zincrby("s", orderInfo.getProductPrice(), sid);
        //对商品所属的品牌进行累加
        jedis.zincrby("p", orderInfo.getProductPrice(), pid);

        jedis.close();

        //打印品类前三榜单
        Set<String> c_s = jedis.zrange("c",0,2);
        for(String c:c_s){
            System.out.println("品类前三榜单");
            System.out.println(c+"      排名" +jedis.zrank("c",c)+"     金额："+jedis.zscore("c",c));
        }

        //打印店铺前三榜单
        Set<String> s_s = jedis.zrange("s",0,2);
        for(String s:s_s){
            System.out.println("品类前三榜单");
            System.out.println(s+"      排名" +jedis.zrank("s",s)+"     金额："+jedis.zscore("s",s));
        }
        //打印品牌前三榜单
        Set<String> p_s = jedis.zrange("p", 0, 2);
        for(String p:p_s){
            System.out.println("品类前三榜单");
            System.out.println(p+"      排名" +jedis.zrank("p",p)+"     金额："+jedis.zscore("p",p));
        }
    }

    private String getBubyProductId(String productId,String type){

        int id = Integer.parseInt(productId);
        if(id<20000){
            return appMap.get(type);
        }else if(id<40000){
            return phoneMap.get(type);
        }else if(id<60000){
            return manMap.get(type);
        }else if(id<80000){
            return beautyMap.get(type);
        }else {
            return automobileMap.get(type);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
