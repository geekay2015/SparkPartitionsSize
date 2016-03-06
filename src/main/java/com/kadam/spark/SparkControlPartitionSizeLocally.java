package com.kadam.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


/**
 * Created by gangadharkadam on 3/5/16.
 *
 *
 * Project Name: $(PROJECT_NAME}
 */

public class SparkControlPartitionSizeLocally {
    public static void main( String[] args ) throws Exception
    {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkControlPartitionSizeLocally");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        String input = "four score and seven years ago our fathers "
                + "brought forth on this continent "
                + "a new nation conceived in liberty and "
                + "dedicated to the propostion that all men are created equal";

        List<String> lst = Arrays.asList(StringUtils.split(input, ' '));

        for (int i = 1; i < 30; i++) {
            JavaRDD<String> stringRDD = sc.parallelize(lst,i);
            System.out.println(stringRDD.partitions().size());
        }





        //stop the context
        sc.stop();
    }
}
