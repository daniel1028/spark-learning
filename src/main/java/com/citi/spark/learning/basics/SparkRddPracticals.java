package com.citi.spark.learning.basics;

import com.citi.spark.learning.util.Util;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRddPracticals implements SparkContextConnector {
    @Override
    public void execute(JavaSparkContext context) {

        JavaRDD<String> intitialRdd = context.textFile("C:\\Users\\esscay\\IdeaProjects\\spark-learning\\src\\main\\resources\\inputs\\input.txt");

        //Map the line which has only alphabets
        JavaRDD<String> lettersOnly = intitialRdd.map(sentences -> sentences.replaceAll("[^a-zA-z\\s]", "").toLowerCase());
        //filter-out empty lines
        JavaRDD<String> validLinesOnly = lettersOnly.filter(sent -> sent.trim().length() > 0);
        //split sentences word by word and convert as single RDD using flatMap
        JavaRDD<String> wordsOnly = validLinesOnly.flatMap(words -> Arrays.asList(words.split(" ")).iterator());
        //filter-out empty word
        JavaRDD<String> filteredBlankWords = wordsOnly.filter(words -> words.trim().length() > 0);
        //filter only valid boring words which we have in the file
        JavaRDD<String> notBorningWords = filteredBlankWords.filter(Util::isNotBoring);
        //create tuple for each word with count 1. Tuples is he key-value pair like Hashmap. but it allows duplicate key
        JavaPairRDD<String, Integer> pairRdd = notBorningWords.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        //reduce by word to find the occurences for each word
        JavaPairRDD<String, Integer> boringWordsCount = pairRdd.reduceByKey((val1, val2) -> (val1 + val2));
        //switch count and word by applying into new tuple
        JavaPairRDD<Integer, String> countWithWords = boringWordsCount.mapToPair(wordCountToSwitch -> new Tuple2<Integer, String>(wordCountToSwitch._2, wordCountToSwitch._1));
        //now key will be count and sort the tuples by key.
        JavaPairRDD<Integer, String> sortedWords = countWithWords.sortByKey(false); // default true (asc). false is descending order.
        //Take first 100 records and print
        sortedWords.take(100).forEach(System.out::println);


        //all the above line can written in one line as below.
        intitialRdd.map(sentences -> sentences.replaceAll("[^a-zA-z\\s]", "").toLowerCase())
                .filter(sent -> sent.trim().length() > 0)
                .flatMap(words -> Arrays.asList(words.split(" ")).iterator())
                .filter(words -> words.trim().length() > 0)
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((val1, val2) -> (val1 + val2))
                .mapToPair(wordCountToSwitch -> new Tuple2<Integer, String>(wordCountToSwitch._2, wordCountToSwitch._1))
                .sortByKey(false)
                .take(100)
                .forEach(System.out::println);
    }
}
