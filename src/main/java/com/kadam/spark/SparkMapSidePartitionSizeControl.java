package com.kadam.spark;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;


/**
 * Created by gangadharkadam on 3/5/16.
 *
 *
 * Project Name: $(PROJECT_NAME}
 */

public class SparkMapSidePartitionSizeControl {
    public static int ONE_MB = 1024*1024;

    public static void controlMapsidePartitionSize(
            String inputPath,
            String outputPath,
            int noOfPartitions,
            long batchSize,
            long minpartitionsSize) throws Exception {

        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("partitioncontrolsize");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        if (batchSize > 0) {
            sc.hadoopConfiguration().setLong("fs.local.block.size", batchSize);
        }

        if (minpartitionsSize > 0) {
            sc.hadoopConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize", minpartitionsSize);
        }
        FileUtils.deleteQuietly(new File(outputPath));

        JavaRDD<String> inputRDD = null;

        if (noOfPartitions>0) {
            inputRDD = sc.textFile(inputPath, noOfPartitions);
        }
        else {
            inputRDD = sc.textFile(inputPath);
        }

        System.out.println(inputRDD.partitions().size());

        JavaRDD<String> mapRDD = inputRDD.map(x -> x);
        mapRDD.saveAsTextFile(outputPath);


        //Stop the context
        sc.stop();
    }

    //Goal Size
    public static long getGoalSize(String inputPath, int noOfPartitions) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(inputPath), true);
        long totalSize = 0;

        while (files.hasNext()) {
            FileStatus f = files.next();

            if (!f.isDirectory()) {
                totalSize += f.getLen();
            }
        }

        System.out.println("Total Size in MB="+totalSize / ONE_MB);
        long goalSize = Math.round((float)totalSize / (noOfPartitions == 0 ? 1 : noOfPartitions));
        return goalSize;

    }

    //Compute the partition size
    public static long computePartitionSize(
            long goalSize,
            long minPartitionSize,
            long blockSize) {
        if (blockSize==0) {
            blockSize = 32 * 1024 * 1024;
        }
        if (minPartitionSize==0){
            minPartitionSize = 1;
        }
        long partitionSize = Math.max(minPartitionSize, Math.min(goalSize, blockSize));
        System.out.println(
                partitionSize+"=Math.max(minSize="+minPartitionSize+", Math.min(goalSize="+goalSize+", blockSize="+blockSize+"))");
        return partitionSize;


    }

    //print the size of each partition
    public static void printSizeOfEachPartition(
            String inputPath,
            long partitionSize,
            boolean splitOverall) throws IOException {
        Assert.assertTrue(partitionSize > 0);
        FileSystem fs = FileSystem.get(new Configuration());
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(inputPath), true);

        List<Long> partitions = new ArrayList<Long>();
        while (files.hasNext()) {
            FileStatus f = files.next();
            if (!f.isDirectory()) {
                long fSize = f.getLen();
                System.out.println(
                        "Input File Name:"+f.getPath().toString()+"="+fSize
                );

                while (fSize >= partitionSize) {
                    partitions.add(partitionSize);
                    fSize = fSize - partitionSize;
                }

                if (fSize > 0) {
                    partitions.add(fSize);
                    fSize=fSize - partitionSize;
                }

            }
        }

        int partitionNo = 0;
        long pSize = 0;
        for (long p:partitions){
            if (!splitOverall){
                pSize = p;
                System.out.println("Partition Index="+partitionNo+", Partition Size="+pSize/ONE_MB);
                partitionNo++;
            }
            else {
                if (p < partitionSize){
                    pSize = pSize + p;
                }
                else {
                    pSize = pSize + p;
                    System.out.println("Partition Index="+partitionNo+", Partition Size="+pSize/ONE_MB);
                    pSize = 0;
                    partitionNo++;
                }
            }
        }

        if (splitOverall && pSize > 0) {
            System.out.println("Partition Index=" + partitionNo + ", Partition Size=" + pSize / ONE_MB);
        }
    }

    //Main Method
    public static void main(String[] args) throws Exception {
        System.out.println(System.getProperty("hadoop.home.dir"));

        String inputPath = args[0];
        String baseOutputPath = args[1];



        int scenarioIndex=1;

        /*
		 * Scenario 1=Default
		 * NoOfPartions = Default
		 * Block Size In Local Mode = 32MB
		 * minPartitionSize=1 byte(Defined in Hadoop code)
		 */

        System.out.println("****"+scenarioIndex+"*************");

        int noOfPartitions = 0;
        long blockSize = 0; //32 MB by default
        long minPartitionSize = 0;
        long goalSize = getGoalSize(inputPath,noOfPartitions);
        long partitionSize = computePartitionSize(goalSize, minPartitionSize, blockSize);
        System.out.println("Partition Size in MB="+partitionSize/ONE_MB);
        scenarioIndex++;

        /*
         * Scenario 2=Control Number of Partitions.
         * NoOfPartions = 30
         * Block Size In Local Mode = 32 MB
         * minPartitionSize=1 byte(Defined in Hadoop code)
         */
        System.out.println("****"+scenarioIndex+"*************");
        noOfPartitions = 30;
        blockSize = 0; //default
        minPartitionSize = 0; //default
        goalSize = getGoalSize(inputPath, noOfPartitions);
        partitionSize = computePartitionSize(goalSize, minPartitionSize, blockSize);
        System.out.println("Partition Size in MB="+partitionSize/ONE_MB);

        SparkMapSidePartitionSizeControl.controlMapsidePartitionSize(
                inputPath,
                baseOutputPath+scenarioIndex,
                noOfPartitions,
                blockSize,
                minPartitionSize
        );
        scenarioIndex++;

        /*
         * Scenario 3=Increase Partition Size(The Wrong Way)
         * NoOfPartions = 5
         * Block Size In Local Mode = 32MB
         * minPartitionSize=1 byte(Defined in Hadoop code)
         */

        System.out.println("****"+scenarioIndex+"*************");
        noOfPartitions = 5;
        blockSize = 0;
        minPartitionSize = 0;
        goalSize = getGoalSize(inputPath, noOfPartitions);
        partitionSize = computePartitionSize(goalSize, minPartitionSize, blockSize);

        System.out.println("Partition Size in MB="+partitionSize/ONE_MB);

        SparkMapSidePartitionSizeControl.controlMapsidePartitionSize(
                inputPath,
                baseOutputPath + scenarioIndex,
                noOfPartitions,
                blockSize,
                minPartitionSize);
        scenarioIndex++;


        /*
         * Scenario 4=Increase Partition Size(The Right Way)
         * NoOfPartions = 0
         * Block Size In Local Mode = 32MB
         * minPartitionSize= 64MB
         */
        System.out.println("****"+scenarioIndex+"*************");
        noOfPartitions = 0;
        blockSize = 0;
        minPartitionSize = 64 * 1024 * 1024;
        goalSize = getGoalSize(inputPath, noOfPartitions);
        partitionSize = computePartitionSize(goalSize,minPartitionSize,blockSize);

        System.out.println("Partition Size in MB="+partitionSize/ONE_MB);

        SparkMapSidePartitionSizeControl.controlMapsidePartitionSize(
                inputPath,
                baseOutputPath + scenarioIndex,
                noOfPartitions,
                blockSize,
                minPartitionSize);
        scenarioIndex++;

    }
}
