package club.cleland.spark_learning.spark_sql.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;


public class UDFJava {
    public static SparkSession getSession(){
        SparkSession spark = SparkSession
                .builder()
                .appName("UDFJava")
                .master("local[1]")
                .getOrCreate();
        return spark;
    }

    public static void main(String[] args) throws Exception{
            strUpper();
    }

    /**
     * strUpper UDF: 全部转化为大写；
     */
    public static void strUpper(){
        SparkSession spark = getSession();
        Dataset<Row> personDF = spark.read().option("multiline", "true").json(getResource("person.json").getAbsolutePath());

        spark.udf().register("strUpper", new UDF1<String, String>() {
            public String call(String word) throws Exception {
                return word.toUpperCase();
            }
        }, DataTypes.StringType);
        personDF.createOrReplaceTempView("person");
        Dataset<Row> resultDF = spark.sql("select strUpper(name) as Name from person");
        resultDF.show(10);
    }

    /**
     * 获取resousces里的文件
     * @param file
     * @return
     */
    private static final File getResource(String file) {
        return new File(UDFJava.class.getClassLoader().getResource(file).getPath());
    }
}
