package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Int;
import scala.Tuple2;

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
//        map();
//        filter();
//        flatMap();
//        groupByKey();
//        reduceByKey();
//        sortByey();
//        join();
//        cogroup();
//        sample();
//        union();
//        intersection();
//        distinct();
        aggregareByKey();
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
     * class_1 90      groupbykey  <class_1,(90,99,86)>  <class_2,(78,76,90>
     * class_2 78
     * class_1 99
     * class_2 76
     * class_2 90
     * class_1 86
     */
    public static void groupByKey(){
        JavaSparkContext sc = getSc();
        JavaPairRDD rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("class_1", 90),
                new Tuple2<String, Integer>("class_2", 78),
                new Tuple2<String, Integer>("class_1", 99),
                new Tuple2<String, Integer>("class_2", 76),
                new Tuple2<String, Integer>("class_2", 90),
                new Tuple2<String, Integer>("class_1", 86)
        ));

        JavaPairRDD groupRDD = rdd.groupByKey();
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable>>() {
            public void call(Tuple2<String, Iterable> o) throws Exception {
                System.out.println(o._1);
                Iterator iterator = o._2.iterator();
                while(iterator.hasNext()){
                    System.out.println(iterator.next());
                }
            }
        });
    }

    /**
     * class_1 90      求和：reducebykey  <class_1,sum(90,99,86)>  <class_2, sum(78,76,90>
     * class_2 78
     * class_1 99
     * class_2 76
     * class_2 90
     * class_1 86
     */
    public static void reduceByKey(){
        JavaSparkContext sc = getSc();
        JavaPairRDD rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("class_1", 90),
                new Tuple2<String, Integer>("class_2", 78),
                new Tuple2<String, Integer>("class_1", 99),
                new Tuple2<String, Integer>("class_2", 76),
                new Tuple2<String, Integer>("class_2", 90),
                new Tuple2<String, Integer>("class_1", 86)
        ));

        JavaPairRDD reduceRDD = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2 value) throws Exception {
                System.out.println("key: " + value._1 + " value: " + value._2);
            }
        });

    }

    /**
     * <90,henry>
     * <88,henry>    ->   <88,henry> <90,henry>
     */
    public static void sortByey(){
        JavaSparkContext sc = getSc();
        JavaPairRDD rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, String>(88, "henry"),
                new Tuple2<Integer, String>(90, "henry")
        ));

        JavaPairRDD sortedRDD = rdd.sortByKey(true);
        sortedRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2 value) throws Exception {
                System.out.println("key: " + value._1 + " value: " + value._2);
            }
        });
    }


    /**
     * 数据集一：(1, henry)    => <1, <herry, 90>>
     * 数据集二：（1, 90）
     */
    public static void join(){
        JavaSparkContext sc = getSc();

        JavaPairRDD rdd1 = sc.parallelizePairs(Arrays.asList(
            new Tuple2<Integer, String>(2,"leo"),
            new Tuple2<Integer, String>(3,"chenry"),
            new Tuple2<Integer, String>(4,"lili")
        ));
        JavaPairRDD rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(2,88),
                new Tuple2<Integer, Integer>(3,99),
                new Tuple2<Integer, Integer>(4,100)
        ));

        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = rdd1.join(rdd2);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1 + " " + integerTuple2Tuple2._2._1 + " " + integerTuple2Tuple2._2._2);
            }
        });
    }

    /**
     * 数据集一 ：(2,leo)                      cogroup =>   <2,<leo,(88,90,55,78)>>
     * 数据集二：(2,88)(2,90)(2,55)(2,78)                   <Integer,Tuple2<Iterable,Iterable>>
     */
    public static void cogroup(){
        JavaSparkContext sc = getSc();

        JavaPairRDD rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, String>(2,"leo"),
                new Tuple2<Integer, String>(3,"chenry"),
                new Tuple2<Integer, String>(4,"lili")
        ));
        JavaPairRDD rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(2,88),
                new Tuple2<Integer, Integer>(2,90),
                new Tuple2<Integer, Integer>(2,55),
                new Tuple2<Integer, Integer>(2,78),
                new Tuple2<Integer, Integer>(3,99),
                new Tuple2<Integer, Integer>(4,100)
        ));

        JavaPairRDD<Integer, Tuple2<Iterable, Iterable>> coRDD = rdd1.cogroup(rdd2);
        coRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable, Iterable>>>() {
            public void call(Tuple2<Integer, Tuple2<Iterable, Iterable>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1 + " " + integerTuple2Tuple2._2._1 + " " + integerTuple2Tuple2._2._2);
            }
        });
    }

    /**
     * 随机抽取
     */
    public static void sample(){
        JavaSparkContext sc = getSc();
        JavaRDD rdd = sc.parallelize(Arrays.asList("cherry", "herry", "leo", "ben", "lili"));
        JavaRDD sampleValues = rdd.sample(false, 0.3);
        for(Object o: sampleValues.collect()){
            System.out.println(o);
        }
    }

    /**
     * union合并
     */
    public static void union(){
        JavaSparkContext sc = getSc();
        JavaRDD rdd1 = sc.parallelize(Arrays.asList("cherry","herry"));
        JavaRDD rdd2 = sc.parallelize(Arrays.asList("ben","leo"));
        JavaRDD unionRDD = rdd1.union(rdd2);

        for(Object o: unionRDD.collect()){
            System.out.println(o);
        }

    }

    /**
     * 获取两个rdd里相同的数据
     */
    public static void intersection(){
        JavaSparkContext sc = getSc();
        JavaRDD rdd1 = sc.parallelize(Arrays.asList("cherry","herry", "tom"));
        JavaRDD rdd2 = sc.parallelize(Arrays.asList("ben","leo", "tom", "herry"));
        JavaRDD interRDD = rdd1.intersection(rdd2);

        for(Object o: interRDD.collect()){
            System.out.println(o);
        }
    }

    public static void distinct(){
        JavaSparkContext sc = getSc();
        JavaRDD rdd = sc.parallelize(Arrays.asList("cherry","herry", "tom", "herry", "tom"));
        JavaRDD distRDD = rdd.distinct();

        for(Object o: distRDD.collect()){
            System.out.println(o);
        }
    }

    /**
     * 与reduceByKey类似，只是aggregareByKey做reduce，需要明细指明：单个map里的聚合方式和最终的聚合方式
     */
    public static void aggregareByKey(){
        JavaSparkContext sc = getSc();
        JavaRDD rdd = sc.parallelize(Arrays.asList("hadoop", "hadoop", "python", "spark", "hadoop", "python"), 2);
        JavaPairRDD words = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2 call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });

        JavaPairRDD reduceWords = words.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        for(Object o: reduceWords.collect()){
            System.out.println(o);
        }
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
