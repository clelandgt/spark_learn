package club.cleland.spark_learn.kfk_log_analyze;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;


public class LogApp {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("LogApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile("hdfs://header:8020/data/input/access.log");

        JavaPairRDD<String, LogInfo> mapPairRDD = mapToPairRDD(rdd);
        JavaPairRDD<String, LogInfo> aggreRDD = aggreByDeviceID(mapPairRDD);
        JavaPairRDD<LogSort, String> mapPairSortRDD = mapToPairSortRDD(aggreRDD);

        JavaPairRDD<LogSort, String> sortRDD = mapPairSortRDD.sortByKey(false);
        List<Tuple2<LogSort, String>> list = sortRDD.take(10);
        for(Tuple2<LogSort, String> logSortStringTuple2: list){
            System.out.println(
                    " deviceID: " + logSortStringTuple2._2
                            +  " upTraffic: " + logSortStringTuple2._1.getUpTraffic()
                            +  " downTraffic: " + logSortStringTuple2._1.getDownTraffic()
                            +  " timeStamp: " + logSortStringTuple2._1.getTimeStamp()

            );
        }

        //printAggreByDeviceID(aggreRDD);
    }

    /**
     * rdd 映射成<key,value> <deviceID, LogInfo>
     * @param rdd
     * @return
     */
    private static JavaPairRDD<String, LogInfo> mapToPairRDD(JavaRDD<String> rdd){
        return rdd.mapToPair(new PairFunction<String, String, LogInfo>() {
            public Tuple2<String, LogInfo> call(String line) throws Exception {
                long timeStamp = Long.valueOf(line.split("\t")[0]);
                String deviceID = String.valueOf(line.split("\t")[1]);
                long upTraffic = Long.valueOf(line.split("\t")[2]);
                long downTraffic = Long.valueOf(line.split("\t")[3]);

                LogInfo logInfo = new LogInfo(timeStamp, upTraffic, downTraffic);
                return new Tuple2(deviceID, logInfo);
            }
        });
    }

    /**
     * 按照deviceID聚合，timeStamp求最小值，upTraffic求和，downTraffic求和
     * @param mapPairRDD
     * @return
     */
    private static JavaPairRDD<String, LogInfo> aggreByDeviceID(JavaPairRDD<String, LogInfo> mapPairRDD){
        return mapPairRDD.reduceByKey(new Function2<LogInfo, LogInfo, LogInfo>() {
            public LogInfo call(LogInfo logInfo1, LogInfo logInfo2) throws Exception {
                long timeStamp;
                if(logInfo1.getTimeStamp() < logInfo2.getTimeStamp()){
                    timeStamp = logInfo1.getTimeStamp();
                }else{
                    timeStamp = logInfo2.getTimeStamp();
                }
                long downTraffic = logInfo1.getDownTraffic() + logInfo2.getDownTraffic();
                long upTraffic = logInfo1.getUpTraffic() + logInfo2.getUpTraffic();

                LogInfo logInfo = new LogInfo(timeStamp, upTraffic, downTraffic);
                return logInfo;
            }
        });
    }

    /**
     * 打印：按照设备deviceID聚合后的数据
     * @param reduceByKey
     */
    private static void printAggreByDeviceID(JavaPairRDD<String, LogInfo> reduceByKey){
        reduceByKey.foreach(new VoidFunction<Tuple2<String, LogInfo>>() {
            public void call(Tuple2<String, LogInfo> stringLogInfoTuple2) throws Exception {
                System.out.println(stringLogInfoTuple2._1 + " = " + stringLogInfoTuple2._2);

                System.out.println(
                        " deviceID: " + stringLogInfoTuple2._1
                                +  " timeStamp: " + stringLogInfoTuple2._2.getTimeStamp()
                                +  " upTraffic: " + stringLogInfoTuple2._2.getUpTraffic()
                                +  " downTraffic: " + stringLogInfoTuple2._2.getDownTraffic()

                );
            }
        });
    }

    /**
     * 转化PairRDD <String, LogInfo> -> <LogSort, String>
     * @param aggreRDD
     * @return
     */
    private static JavaPairRDD<LogSort, String> mapToPairSortRDD(JavaPairRDD<String, LogInfo> aggreRDD){
        return aggreRDD.mapToPair(new PairFunction<Tuple2<String, LogInfo>, LogSort, String>() {
            public Tuple2<LogSort, String> call(Tuple2<String, LogInfo> stringLogInfoTuple2) throws Exception {
                String deviceID = stringLogInfoTuple2._1;
                long timeStamp = stringLogInfoTuple2._2.getTimeStamp();
                long upTraffic = stringLogInfoTuple2._2.getUpTraffic();
                long downTraffic = stringLogInfoTuple2._2.getDownTraffic();
                LogSort logSort = new LogSort(timeStamp, upTraffic, downTraffic);
                return new Tuple2(logSort, deviceID);
            }
        });
    }
}
