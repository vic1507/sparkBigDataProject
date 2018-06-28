package itwiki.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Sparkalo {
	
	public static void main (String [] args)
	{
		
		SparkSession sparkSession = SparkSession.builder().appName("Simple Application").getOrCreate();
		
		JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<String> inputFile = context.textFile("author", 1);
		JavaPairRDD<String, String> pairs = inputFile.mapToPair(row -> new Tuple2<String, String>(row.split("\t")[4], row.split("\t")[2]));
		
		Map<String, Iterable<String>> map = pairs.groupByKey().collectAsMap();
		Map<String, JavaPairRDD<Integer, String> > results = new HashMap<>();
		
		for (String author : map.keySet())
		{
			List<Tuple2<String, Integer>> tuplesToCount = new ArrayList<Tuple2<String,Integer>>();
			for (String category : map.get(author))
				tuplesToCount.add(new Tuple2<String, Integer>(category, 1));
			
			JavaPairRDD<String,Integer> tuplesToCountPair = context.parallelizePairs(tuplesToCount);
			JavaPairRDD<Integer, String> swappedPairSortByKey =context.parallelizePairs(tuplesToCountPair.reduceByKey((i1,i2)-> i1+i2).mapToPair(item -> item.swap()).sortByKey(false).take(5));
			results.put(author, swappedPairSortByKey);
		}
		
		for (String author : results.keySet())
		{
			results.get(author).foreach(line -> System.out.println("Author : " + author + "Category : " + line._2 + " Value :" + line._1));
		}
		
		context.close();
		sparkSession.stop();
	}
}
