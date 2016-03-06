package com.kadam.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Created by gangadharkadam on 3/5/16.
 *
 * Project Name: MasteringSpark
 * RDDs have two fundamental types of operations: Transformations and Actions
 * Transformations are lazy operations on a RDD that return RDD objects or collections of RDDs
 * e.g. map, filter, reduceByKey, join, cogroup, randomSplit, etc
 *  Transformations are lazy and are not executed immediately, but only after an action has been executed
 *
 *  There are two kinds of transformations:
 *  Narrow Transformations:-
 *  1.are the result of map, filter.
 *  2.The data required to compute resides in singles partitions of the parent RDD.
 *  3.An output RDD has partitions with records that originate from a single partition in the parent RDD.
 *  4.Only a limited subset of partitions used to calculate the result.
 *  5.Spark groups narrow transformations as a stage.
 *
 *  Wide Transformation:-
 *  1.are the result of groupByKey and reduceByKey
 *  2.The data required to compute the records in a single partition may reside in many partitions of the parent RDD
 *  3.All of the tuples with the same key must end up in the same partition, processed by the same task
 *  4.To satisfy these operations, Spark must execute RDD shuffle,
 *  which transfers data across cluster and results in a new stage with a new set of partitions
 */


object RDDTransformations {

  def main(args: Array[String]) {

    //Define the Spark Configuration
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local")

    //Define the Spark Context
    val sc = new SparkContext(conf)

    //Define the SQL Context
    val sqlContext = new SQLContext(sc)

    val file = sc.textFile("hdfs://localhost:9000/README.md")

    val allWords = file.flatMap(_.split("\\s+"))

    //Narrow transformation
    val words = allWords.filter(!_.isEmpty)

    //Narrow transformation
    val pairs = words.map((_,1))

    val redcudeByKey = pairs.reduceByKey(_ + _)

    redcudeByKey.toDebugString


    val top10words = redcudeByKey.takeOrdered(10)(Ordering[Int].reverse.on(_._2)).foreach(println)


    //stop the context
    sc.stop()
  }


}
