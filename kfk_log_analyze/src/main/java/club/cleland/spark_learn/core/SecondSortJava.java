package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class SecondSortJava {

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

    /**
     * class1 67,
     * class2 89,
     * class1 78,       ->  class1 (67,78,99)  class2 (89,99)
     * class2 90,
     * class1 99
     */
    public static void main(String[] args){
        JavaSparkContext sc = getSc();
        JavaRDD rdd = sc.parallelize(Arrays.asList(
                "class1 67",
                "class2 89",
                "class1 78",
                "class2 90",
                "class1 99"
        ));

        JavaPairRDD<String, Integer> beforeSordRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2 call(String line) throws Exception {
                String first = line.split(" ")[0];
                Integer second = Integer.valueOf(line.split(" ")[1]);
                return new Tuple2(first, second);
            }
        });

        JavaPairRDD<String, Integer> sortedRDD = beforeSordRDD.sortByKey(false);
        sortedRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ":" + stringIntegerTuple2._2);
            }
        });
    }
}
