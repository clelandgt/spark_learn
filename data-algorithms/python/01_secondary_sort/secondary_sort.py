# -*- coding: utf-8 -*-
# @File  : secondary_sort.py
# @Author: clelandgt@163.com
# @Date  : 2020-04-06
# @Desc  :
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as F, Window


def init_spark():
    config = SparkConf()
    # config.set('spark.dynamicAllocation.maxExecutors', '8')
    # config.set('spark.driver.memory', '4G')
    # config.set('spark.executor.memory', '8G')
    # config.set('spark.executor.cores', '8')
    # config.set('spark.yarn.executora.memoryOverhead', '4G')
    # config.set('spark.sql.shuffle.partitions', '500')
    # config.set('spark.default.parallelism', '500')
    # config.set('spark.port.maxRetries', '1000')
    # config.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    config.set('spark.master', 'local[4]')
    spark = SparkSession.builder.config(conf=config).getOrCreate
    return spark


def main():
    spark = init_spark()


if __name__ == '__main__':
    main()