package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;


public class TransformationJava {
    private SparkSession spark;
    private JavaSparkContext sc;
    public TransformationJava() {
        this.spark = SparkSession
                .builder()
                .appName("Transformtion Function")
                .master("local[1]")
                .getOrCreate();
        this.sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("INFO");
    }

    public static void main(String[] args){
        TransformationJava transformationJava = new TransformationJava();
        transformationJava.map();
    }

    /**
     * 对数据集合求平方：(1,2,3,4,5) -> (1,4,9,16,25)
     */
    public void map(){
        JavaRDD<Integer> rdd = this.sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> resultRdd = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer * integer;
            }

        });

        resultRdd.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }


}
