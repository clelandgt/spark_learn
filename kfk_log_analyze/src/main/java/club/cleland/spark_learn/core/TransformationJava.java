package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;


public class TransformationJava {
    private SparkSession spark;
    private JavaSparkContext sc;

    public static JavaSparkContext getSc(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Transformtion Function")
                .master("local[1]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("INFO");
        return sc;

    }

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
        map();
        filter();
        flatMap();
    }

    /**
     * 对数据集合求平方：(1,2,3,4,5) -> (1,4,9,16,25)
     */
    public static void map(){
        JavaSparkContext sc = getSc();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> resultRDD = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer * integer;
            }
        });
        prinfRDD(resultRDD);
    }

    /**
     * 返回数据集中的偶数
     */
    public static void filter(){
        JavaSparkContext sc = getSc();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));
        JavaRDD<Integer> filterRDD = rdd.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        prinfRDD(filterRDD);
    }

    /**
     * 切分文本的单词
     */
    public static void flatMap(){
        JavaSparkContext sc = getSc();
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hadoop,hive,spark", "python,spark,hive"));
        JavaRDD<String> flatRDD = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });

        prinfRDD(flatRDD);
    }

    /**
     * 打印RDD
     *@param rdd
     */
    public static void prinfRDD(JavaRDD rdd){
        rdd.foreach(new VoidFunction() {
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }
}
