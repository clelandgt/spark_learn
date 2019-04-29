package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class PersistJava {
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

    public static void main(String[] args) {
        JavaSparkContext sc = getSc();
        // 读取本地文件
        JavaRDD rdd = sc.textFile("file:///Users/cleland/Desktop/access.log").cache();
        long begin1 = System.currentTimeMillis();
        System.out.println(rdd.count());
        System.out.println(System.currentTimeMillis() - begin1);

        long begin2 = System.currentTimeMillis();
        System.out.println(rdd.count());
        System.out.println(System.currentTimeMillis() - begin2);
    }
}
