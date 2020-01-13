package org.vjsolutions.spark.prep;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkWordCount {

    public static void main(String args[]){

        System.setProperty("hadoop.home.dir", "C:\\SparkSampleTest");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkWordCount");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputFile = sc.textFile("C:\\SparkSampleTest\\test.txt");

        JavaRDD<String> wordFromInput = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        JavaPairRDD countData = wordFromInput.mapToPair(x -> new Tuple2(x,1)).reduceByKey((x,y) -> (int)x + (int)y);

        countData.saveAsTextFile("CountData3");

    }
}
