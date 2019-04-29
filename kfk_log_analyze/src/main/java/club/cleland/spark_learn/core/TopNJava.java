package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TopNJava {

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
        JavaRDD rdd = sc.parallelize(Arrays.asList(23,12,56,44,23,99,13,57));
        JavaPairRDD<Integer, Integer> befortSortRDD = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2 call(Integer value) throws Exception {
                return new Tuple2(value, value);
            }
        });

        JavaPairRDD<Integer, Integer> sortRDD = befortSortRDD.sortByKey(true);
        JavaRDD beginTop = sortRDD.map(new Function<Tuple2<Integer, Integer>,Integer>() {
            public Integer call(Tuple2<Integer, Integer> o) throws Exception {
                return o._1;
            }
        });

        List<Integer> list =  beginTop.take(5);
        System.out.println(list);
    }
}
