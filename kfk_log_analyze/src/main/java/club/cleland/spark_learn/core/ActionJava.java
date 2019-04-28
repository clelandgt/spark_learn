package club.cleland.spark_learn.core;

import net.jpountz.util.SafeUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionJava {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }

    public static void main(String[] args){
//        countByKey();
//        take();
//        count();
//        collect();
        reduce();
    }

    public static void countByKey(){
        JavaSparkContext sc = getsc();
        JavaPairRDD rdd = sc.parallelizePairs(Arrays.asList(
            new Tuple2<String, String>("class_2","henry"),
            new Tuple2<String, String>("class_1","cherry"),
            new Tuple2<String, String>("class_1","ben"),
            new Tuple2<String, String>("class_2","lili")
        ));

        Map<String, Long> values = rdd.countByKey();
        for(Map.Entry ent: values.entrySet()){
            System.out.println("key: " + ent.getKey() + " value: " + ent.getValue());
        }
    }

    public static void save(){
        JavaSparkContext sc = getsc();
        JavaRDD rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        rdd.saveAsTextFile("hdfs://header:8020/data/output");
    }

    public static void take(){
        JavaSparkContext sc = getsc();
        JavaRDD rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        List<Integer> list = rdd.take(2);
        for(Integer value: list){
            System.out.println(value);
        }
    }

    public static void count(){
        JavaSparkContext sc = getsc();
        JavaRDD rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        System.out.println(rdd.count());
    }

    public static void collect(){
        JavaSparkContext sc = getsc();
        JavaRDD rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));

        List<Integer> values = rdd.collect();
        for(Integer value: values){
            System.out.println(value);
        }
    }

    public static void reduce(){
        JavaSparkContext sc = getsc();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));

        Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);
    }

}
