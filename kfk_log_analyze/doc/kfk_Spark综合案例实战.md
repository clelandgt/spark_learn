## 需求
1. 计算每个设备(deviceID)总上行流量之和与下行流量之和(时间戳取最小的)
2. 根据上行流量与下行流量进行排序
    -- 优先根据上行流量进行排序，如果上行流量相等，那么根据下行流量排序。如果上行流量与下行流量相当，那么根据最早时间戳类排序。即二次排序
3. 获取流量最大的前10个设备


时间戳           deviceID                            上行流量 下行流量
1454307391161	77e3c9e1811d4fb291d0d9bbd456bb4b	79976	11496


## 运行

``` shell
$ spark-submit --class club.cleland.spark_learn.kfk_log_analyze.WordCountJava  kfk_log_analyze-1.0-SNAPSHOT.jar
```