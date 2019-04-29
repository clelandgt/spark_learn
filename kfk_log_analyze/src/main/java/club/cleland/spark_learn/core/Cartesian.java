package club.cleland.spark_learn.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class Cartesian {
    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }

    public static void main(String[] args){
        JavaSparkContext sc = getsc();
        JavaRDD rdd1 = sc.parallelize(Arrays.asList("衣服-1", "衣服-2"));
        JavaRDD rdd2 = sc.parallelize(Arrays.asList("裤子-1", "裤子-2"));

        JavaPairRDD carteValues = rdd1.cartesian(rdd2);

        carteValues.foreach(new VoidFunction() {
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }
}
