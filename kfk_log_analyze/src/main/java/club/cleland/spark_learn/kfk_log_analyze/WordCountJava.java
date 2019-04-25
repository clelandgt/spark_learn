package club.cleland.spark_learn.kfk_log_analyze;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountJava {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD lines = sc.textFile("hdfs://header:8020/data/input/words.txt");
        JavaRDD words  =  lines.flatMap(new FlatMapFunction<String,String>() {

            public Iterator call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD word = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2 call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });

        JavaPairRDD wordCount = word.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> o) throws Exception {
                System.out.println(o._1 + " : " + o._2);
            }
        });

    }
}
