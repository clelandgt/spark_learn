package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class GroupTopNJava {
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
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "class1 67",
                "class2 78",
                "class1 78",
                "class1 99",
                "class1 109",
                "class1 34",
                "class1 45",
                "class2 34",
                "class2 88",
                "class2 98",
                "class2 33"
        ));
        JavaPairRDD<String, Integer> beginGroup = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String line) throws Exception {
                String key = line.split(" ")[0];
                String value = line.split(" ")[1];
                return new Tuple2(key, value);
            }
        });

        JavaPairRDD groupValues = beginGroup.groupByKey();
        System.out.println(groupValues.collect());
    }

}
