package com.kadam.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by gangadharkadam on 3/5/16.
 * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
 *
 * If any of the RDDs already has a partitioner, choose that one.
 *
 * Otherwise, we use a default HashPartitioner. For the number of partitions, if
 * spark.default.parallelism is set, then we'll use the value from SparkContext
 * defaultParallelism, otherwise we'll use the max number of upstream partitions.
 *
 * Unless spark.default.parallelism is set, the number of partitions will be the
 * same as the number of partitions in the largest upstream RDD, as this should
 * be least likely to cause out-of-memory errors.
 *
 * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
 */


public class RDDPartitionsJava {

    public static void main( String[] args ) throws Exception
    {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("RDDPartitionsJava");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // sc is an existing JavaSparkContext.
        SQLContext sqlContext = new SQLContext(sc);

        //Load the input file a string RDD
        JavaRDD<String> rdd = sc.textFile("hdfs://localhost:9000/README.md");

        //Create a pair RDD
        JavaPairRDD<String, Integer> counts = rdd
                .flatMap(x -> Arrays.asList(x.split(" ")))
                .mapToPair(x -> new Tuple2<String, Integer>(x,1))
                .reduceByKey((x,y) -> x + y);
        counts.saveAsTextFile("output");


        //stop the context
                sc.stop();
    }
}
