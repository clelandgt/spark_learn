package club.cleland.spark_learn.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RepartitionJava {
    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("coalesce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }

    public static void main(String[] args){
        JavaSparkContext sc = getsc();

        JavaRDD rdd1 = sc.parallelize(Arrays.asList("henry","chery","ben","leo","lili"), 4);

        // coalesce 前
        JavaRDD mapIndexValues = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator, Iterator>() {
            public Iterator call(Integer index, Iterator iterator) throws Exception {
                List list = new ArrayList();
                while(iterator.hasNext()){
                    list.add(String.valueOf(index) + " " + String.valueOf(iterator.next()));
                }
                return list.iterator();
            }
        }, false);
        for(Object item: mapIndexValues.collect()){
            System.out.println(item);
        }


        // coalesce 后
        JavaRDD coalesceRDD = mapIndexValues.repartition(2);
        JavaRDD mapIndexValues2 = coalesceRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator, Iterator>() {
            public Iterator call(Integer index, Iterator iterator) throws Exception {
                List list = new ArrayList();
                while(iterator.hasNext()){
                    list.add(String.valueOf(index) + " " + String.valueOf(iterator.next()));
                }
                return list.iterator();
            }
        }, false);
        for(Object item: mapIndexValues2.collect()){
            System.out.println(item);
        }
    }
}
