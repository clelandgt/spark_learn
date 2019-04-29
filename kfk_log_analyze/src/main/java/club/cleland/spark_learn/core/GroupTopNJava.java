package club.cleland.spark_learn.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

        /**
         * <class1,(67,78,99,109,34,45,88)>
         *
         * <class1,(109,99,88)>
         */

        JavaPairRDD groupValues = beginGroup.groupByKey();



        /**
         * topN算起演算
         * top(3)， 对于class1
         *
         * null  ->67
         * 78 -> 78,67
         * 99 -> 99,78,67
         * 109 -> 109,99,78 (删除最后一个67)
         * 34 -> 109,99,78(pass)
         * 45 -> 109,99,78(pass)
         * 88 -> 109,99,88(删除最后一个88)
         *
         * ps: 当数据量<3时，且都比当前值小时，将数据插到最后
         * 算法描述：
         * 1. 数据为空直接插入；
         * 2. 当数据 1<=size<3, 从大到小依次比较，出现当前数据比遍历到的数据大时，插入。如果遍历完，都比link中的数据小时，插入最后一条。
         * 3. 当数据 size>=3时: 当出现当前数据比遍历数据大时插入，然后删除最后一条；当没有出现时，就pass
         */

        JavaPairRDD groupTop = groupValues.mapToPair(new PairFunction<Tuple2<String, Iterable>, String, Integer>() {
            public Tuple2 call(Tuple2<String, Iterable> value) throws Exception {
                Iterator iterator = value._2.iterator();
                LinkedList linkedList = new LinkedList();

                while(iterator.hasNext()){
                    int _value = Integer.parseInt(String.valueOf(iterator.next()));
                    if(linkedList.size() == 0){
                        linkedList.add(_value);
                    }else{
                        for(int i = 0; i < linkedList.size(); i++){
                            if(_value > (Integer) linkedList.get(i)){
                                // 遍历到某个元素x出现 _value大于x
                                linkedList.add(i, _value);
                                if(linkedList.size() > 3){
                                    linkedList.removeLast();
                                }
                                break;
                            }else{
                                // 遍历到最后一个元素，当前元素还是小于link的元素
                                if(i == linkedList.size()-1){
                                    if(i<3){
                                        linkedList.add(_value);
                                    }
                                }
                            }
                        }
                    }
                }
                return new Tuple2(value._1, linkedList);
            }
        });

        // 打印遍历后元素
        groupTop.foreach(new VoidFunction<Tuple2<String, List>>() {
            public void call(Tuple2<String, List> o) throws Exception {
                System.out.println(o._1);
                List list = o._2;
                for(Object item: list){
                    System.out.println(item);
                }
            }
        });

    }

}
