#Key Parameters participating in the Partition definition process

#The key parameters used to control the number of partitions are as follows

1.dfs.block.size - 
    The default value in Hadoop 2.0 is 128MB. 
    In the local mode the corresponding parameter is fs.local.block.size (Default value 32MB). It defines the default partition size.
2.numPartitions - 
    The default value is 0 which effectively defaults to 1. 
    This is a parameter you pass to the sc.textFile(inputPath,numPartitions) method. 
    It is used to decrease the partition size (increase the number of partitions) from the default value defined by dfs.block.size
3.mapreduce.input.fileinputformat.split.minsize - 
    The default value is 1 byte. 
    It is used to increase the partition size (decrease the number of partitions) from the default value defined by dfs.block.size
    
#Partition Size Definition
The actual partition size is defined by the following formula in the method
     Math.max(minSize, Math.min(goalSize, blockSize))
where,
minSize is the hadoop parameter mapreduce.input.fileinputformat.split.minsize

blockSize is the value of the dfs.block.size in cluster mode and fs.local.block.size in the local mode

goalSize=totalInputSize/numPartitions
where,
totalInputSize is the total size in bytes of all the files in the input path.
numPartitions is the custom parameter provided to the method sc.textFile(inputPath, numPartitions)     

#print the 4 partition scenarios by invoking the below class 

spark-submit \
→ --master local[*] \
→ --class com.kadam.spark.PrintSparkMapSidePartitionSizeControl \
→ /Users/gangadharkadam/myapps/MasteringSpark/target/scala-2.11/MasteringSpark-assembly-1.0.jar \
→ /user/textdata/

****Scenario 1=Default*************
Total Size in MB=599
33554432=Math.max(minSize=1, Math.min(goalSize=628202432, blockSize=33554432))
Goal Size in MB=599
Partition Size in MB=32
Now printing each file, partition index and the size of the partition
Input File Name:hdfs://localhost:9000/user/textdata/1987.csv=127162942
Input File Name:hdfs://localhost:9000/user/textdata/1988.csv=501039472
Partition Index=0, Partition Size=32
Partition Index=1, Partition Size=32
Partition Index=2, Partition Size=32
Partition Index=3, Partition Size=25
Partition Index=4, Partition Size=32
Partition Index=5, Partition Size=32
Partition Index=6, Partition Size=32
Partition Index=7, Partition Size=32
Partition Index=8, Partition Size=32
Partition Index=9, Partition Size=32
Partition Index=10, Partition Size=32
Partition Index=11, Partition Size=32
Partition Index=12, Partition Size=32
Partition Index=13, Partition Size=32
Partition Index=14, Partition Size=32
Partition Index=15, Partition Size=32
Partition Index=16, Partition Size=32
Partition Index=17, Partition Size=32
Partition Index=18, Partition Size=29
***************************
****Scenario 1.1=Default*************
Total Size in MB=599
67108864=Math.max(minSize=1, Math.min(goalSize=628202432, blockSize=67108864))
Goal Size in MB=599
Partition Size in MB=64
Now printing each file, partition index and the size of the partition
Input File Name:hdfs://localhost:9000/user/textdata/1987.csv=127162942
Input File Name:hdfs://localhost:9000/user/textdata/1988.csv=501039472
Partition Index=0, Partition Size=64
Partition Index=1, Partition Size=57
Partition Index=2, Partition Size=64
Partition Index=3, Partition Size=64
Partition Index=4, Partition Size=64
Partition Index=5, Partition Size=64
Partition Index=6, Partition Size=64
Partition Index=7, Partition Size=64
Partition Index=8, Partition Size=64
Partition Index=9, Partition Size=29
***************************
****Scenario 2*************
Total Size in MB=599
20940082=Math.max(minSize=1, Math.min(goalSize=20940082, blockSize=33554432))
Goal Size in MB=19
Partition Size in MB=19
Now printing each file, partition index and the size of the partition
Input File Name:hdfs://localhost:9000/user/textdata/1987.csv=127162942
Input File Name:hdfs://localhost:9000/user/textdata/1988.csv=501039472
Partition Index=0, Partition Size=19
Partition Index=1, Partition Size=19
Partition Index=2, Partition Size=19
Partition Index=3, Partition Size=19
Partition Index=4, Partition Size=19
Partition Index=5, Partition Size=19
Partition Index=6, Partition Size=21
Partition Index=7, Partition Size=19
Partition Index=8, Partition Size=19
Partition Index=9, Partition Size=19
Partition Index=10, Partition Size=19
Partition Index=11, Partition Size=19
Partition Index=12, Partition Size=19
Partition Index=13, Partition Size=19
Partition Index=14, Partition Size=19
Partition Index=15, Partition Size=19
Partition Index=16, Partition Size=19
Partition Index=17, Partition Size=19
Partition Index=18, Partition Size=19
Partition Index=19, Partition Size=19
Partition Index=20, Partition Size=19
Partition Index=21, Partition Size=19
Partition Index=22, Partition Size=19
Partition Index=23, Partition Size=19
Partition Index=24, Partition Size=19
Partition Index=25, Partition Size=19
Partition Index=26, Partition Size=19
Partition Index=27, Partition Size=19
Partition Index=28, Partition Size=19
Partition Index=29, Partition Size=18
***************************
****Scenario 3=Increase Partition Size(The Wrong Way)*************
Total Size in MB=599
33554432=Math.max(minSize=1, Math.min(goalSize=125640488, blockSize=33554432))
Goal Size in MB=119
Partition Size in MB=32
Now printing each file, partition index and the size of the partition
Input File Name:hdfs://localhost:9000/user/textdata/1987.csv=127162942
Input File Name:hdfs://localhost:9000/user/textdata/1988.csv=501039472
Partition Index=0, Partition Size=32
Partition Index=1, Partition Size=32
Partition Index=2, Partition Size=32
Partition Index=3, Partition Size=25
Partition Index=4, Partition Size=32
Partition Index=5, Partition Size=32
Partition Index=6, Partition Size=32
Partition Index=7, Partition Size=32
Partition Index=8, Partition Size=32
Partition Index=9, Partition Size=32
Partition Index=10, Partition Size=32
Partition Index=11, Partition Size=32
Partition Index=12, Partition Size=32
Partition Index=13, Partition Size=32
Partition Index=14, Partition Size=32
Partition Index=15, Partition Size=32
Partition Index=16, Partition Size=32
Partition Index=17, Partition Size=32
Partition Index=18, Partition Size=29
***************************
****Scenario 4=Increase Partition Size(The Right Way)*************
Total Size in MB=599
67108864=Math.max(minSize=67108864, Math.min(goalSize=628202432, blockSize=33554432))
Goal Size in MB=599
Partition Size in MB=64
Now printing each file, partition index and the size of the partition
Input File Name:hdfs://localhost:9000/user/textdata/1987.csv=127162942
Input File Name:hdfs://localhost:9000/user/textdata/1988.csv=501039472
Partition Index=0, Partition Size=64
Partition Index=1, Partition Size=57
Partition Index=2, Partition Size=64
Partition Index=3, Partition Size=64
Partition Index=4, Partition Size=64
Partition Index=5, Partition Size=64
Partition Index=6, Partition Size=64
Partition Index=7, Partition Size=64
Partition Index=8, Partition Size=64
Partition Index=9, Partition Size=29
***************************

##The four scenarios can be executed by invoking the below class
  
spark-submit \
→ --master local[*] \
→ --class com.kadam.spark.SparkMapSidePartitionSizeControl \
→ /Users/gangadharkadam/myapps/MasteringSpark/target/scala-2.11/MasteringSpark-assembly-1.0.jar \
→ /user/textdata/ /user/partitions/

null
****1*************
Total Size in MB=599
33554432=Math.max(minSize=1, Math.min(goalSize=628202432, blockSize=33554432))
Partition Size in MB=32
****2*************
Total Size in MB=599
20940082=Math.max(minSize=1, Math.min(goalSize=20940082, blockSize=33554432))
Partition Size in MB=19
30
****3*************
Total Size in MB=599
33554432=Math.max(minSize=1, Math.min(goalSize=125640488, blockSize=33554432))
Partition Size in MB=32
5
****4*************
Total Size in MB=599
67108864=Math.max(minSize=67108864, Math.min(goalSize=628202432, blockSize=33554432))
Partition Size in MB=64
5