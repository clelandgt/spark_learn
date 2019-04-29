package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.*;


/**
 * MapPartition类似Map,不同之处在于：Map算子一次就处理一个parttion里一条数据；而mapPartitions算子一次处理一个partition中所有的数据。
 * 如果RDD的数据量不是很大，那么建议采用mapPartitions算子替换map算子。可以加快处理速度；但是RDD数据量较大，比如10亿，那么可以导内存溢出
 */

public class MapPartitionsJava {
    public static JavaSparkContext getSc(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Persist")
                .master("local[1]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("INFO");
        return sc;
    }

    public static void main(String[] args){
        JavaSparkContext sc = getSc();
        JavaRDD rdd = sc.parallelize(Arrays.asList("henry","cherry","leo","ben"), 2);
        System.out.println(rdd.getNumPartitions());
        final Map<String, Double> map = new HashMap<String, Double>();
        map.put("henry",99.4);
        map.put("cherry",79.9);
        map.put("leo",88.3);
        map.put("ben",67.5);

        JavaRDD mapPartition = rdd.mapPartitions(new FlatMapFunction<Iterator, Iterator>() {
            public Iterator call(Iterator o) throws Exception {
                List list = new ArrayList();
                while(o.hasNext()){
                    String name = (String)o.next();
                    Double scroe = (Double) map.get(name);
                    list.add(scroe);
                }
                return list.iterator();
            }
        });

        mapPartition.foreach(new VoidFunction() {
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }
}
