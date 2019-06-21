package com.dgunify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
/**
 * spark-sql-database
 * @author dgunify
 * 将hdfs数据存入数据库
 * Storing HDFS data in database
 */
public final class SparkSqlPutDb {
	public static void main(String[] args) throws Exception {
		try {
			SparkConf sparkConf = new SparkConf().setAppName("oracle1").setMaster("spark://hadoop0:7077");
			JavaSparkContext ctx = new JavaSparkContext(sparkConf);
			//读取hdfs数据
			JavaRDD<String> lines = ctx.textFile("hdfs://hadoop0:9000/datas/doc.txt", 1);
			JavaRDD<Row> mapPartitions = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Row>() {
				private static final long serialVersionUID = 1L;
				public Iterator<Row> call(Iterator<String> iterator) throws Exception {
					ArrayList<Row> arrayList = new ArrayList<Row>();
					while (iterator.hasNext()) {
						String usernam = iterator.next();
						String id = UUID.randomUUID().toString().replace("-","");
						//这里的顺序，需要和下面的sfList.add() 一致。
						//Here's the order, the need and the following sfList.add() Agreement
						Row row = RowFactory.create(id,usernam);
						arrayList.add(row);
					}
					return arrayList.iterator();
				}
			});
			
			Builder bu = SparkSession.builder().appName("oracle1-1").master("spark://hadoop0:7077");// parMap.get("sparkHost")
			SparkSession ss = bu.getOrCreate();
			LinkedList<StructField> sfList = new LinkedList<StructField>();
			//参数1 字段名  参数2字段类型 参数3是否为空校验
			//Parameter 1 field name parameter 2 field type parameter 3 is null check
			sfList.add(DataTypes.createStructField("id", DataTypes.StringType, true));
			sfList.add(DataTypes.createStructField("username", DataTypes.StringType, true));
			StructType schema = DataTypes.createStructType(sfList);
			Dataset<Row> createDataFrame1 = ss.createDataFrame(mapPartitions, schema);
			DataFrameWriter<Row> df = createDataFrame1.write().mode(SaveMode.Append);
			//参数1 数据库连接，参数2 表名 ，参数3 数据库配置信息
			//Parameter 1 database connection, parameter 2 table name, parameter 3 database configuration information
			//mysql，oracle
			df.jdbc(Constant.oracleUrl, "users", Constant.connectionPropertiesOracle);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}