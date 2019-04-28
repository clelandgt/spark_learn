package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/*
使用SparkSQL里的库，需要导入spark-sql。maven导入如下：

<dependency>
	<groupId>org.apache.spark</groupId>
	<artifactId>spark-sql_2.11</artifactId>
	<version>2.1.0</version>
</dependency>
 */

public class Parallelize {
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Parallelize Test")
                .master("local[1]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("DEBUG");

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        Integer result = distData.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(result);
    }
}
