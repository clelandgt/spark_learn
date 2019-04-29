package club.cleland.spark_learn.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.Accumulator;

import java.util.Arrays;

public class VarJava {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("Variable").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }

    public static void main(String[] args){
        //broadcast();
        accumulator();
    }

    /**
     * 广播
     */
    public static void broadcast(){
        JavaSparkContext sc = getsc();
        final Broadcast broadcast = sc.broadcast(5);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> value = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer * (Integer)broadcast.getValue();
            }
        });

        value.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * 累加器
     */
    public static void accumulator(){
        JavaSparkContext sc = getsc();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));

        final Accumulator accumulator = sc.accumulator(0);
        rdd.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                accumulator.add(integer);
            }
        });

        System.out.println(accumulator.value());
    }
}
