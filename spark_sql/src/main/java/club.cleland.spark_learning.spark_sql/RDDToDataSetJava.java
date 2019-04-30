package club.cleland.spark_learning.spark_sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


/**
 * 数据集合 person.txt
 * tom, 90.5
 * jim, 85
 * kylin, 92.0
 */

public class RDDToDataSetJava {
    public static SparkSession getSession(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Persist")
                .master("local[1]")
                .getOrCreate();
        return spark;
    }


    /**
     * RDD转化为Dataset法一：反射的方式
     */
    public static void reflection(){
        SparkSession spark = getSession();
        JavaRDD<Person> personRDD = spark.read()
                .textFile("file:///Users/cleland/gitrooms/spark_learn/spark_sql/person.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person(parts[0], Double.parseDouble(parts[1].trim()));
                    return person;
                });
        Dataset<Row> personDF = spark.createDataFrame(personRDD, Person.class);
        personDF.show();
        personDF.createOrReplaceTempView("person");
        Dataset<Row> filterDF = spark.sql("select * from person where core>90");
        filterDF.show();
    }

    /**
     * RDD转化为Dataset法二：通过编程方式实现
     */
    public static void program(){
        SparkSession spark = getSession();
        JavaRDD<Row> personRDD = spark.read()
                .textFile("file:///Users/cleland/gitrooms/spark_learn/spark_sql/person.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    return RowFactory.create(parts[0], Double.parseDouble(parts[1].trim()));
                });

        List<StructField> fields = new ArrayList<StructField>();
        StructField structFieldName = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField structFieldCore = DataTypes.createStructField("core", DataTypes.DoubleType, true);
        fields.add(structFieldName);
        fields.add(structFieldCore);
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> personDF = spark.createDataFrame(personRDD, schema);
        personDF.show();
    }


    public static void main(String[] args){
//        reflection();
        program();
    }

}
