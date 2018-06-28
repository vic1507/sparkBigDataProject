package itwiki.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Sparkalo {
	
	public static void main (String [] args)
	{
		
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		
		JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> lines = ctx.textFile("author", 1);
		
		JavaPairRDD<String, String> pairs = lines.mapToPair((final String s) -> {
		    String[] tokens = s.split("\t"); 
		    return new Tuple2<String, String>(tokens[4], tokens[2]);
		});
		
		Map<String, Iterable<String>> map = pairs.groupByKey().collectAsMap();
		Map<String, Map<String,Integer>> results = new HashMap<>();
		
		for (String s : map.keySet())
		{
			List<Tuple2<String, Integer>> tuples = new ArrayList<Tuple2<String,Integer>>();
			for (String s2 : map.get(s))
			{
				tuples.add(new Tuple2<String, Integer>(s2, 1));
			}
			
			JavaPairRDD<String,Integer> pairRDD = ctx.parallelizePairs(tuples);
			JavaPairRDD<Integer, String> swappedPair = pairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
		           @Override
		           public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
		               return item.swap();
		           }

		        });
			
			JavaPairRDD<Integer, String> sortByKey = ctx.parallelizePairs(swappedPair.sortByKey(false).take(5));
			JavaPairRDD<String,Integer> ordered = sortByKey.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
		           @Override
		           public Tuple2<String,Integer> call(Tuple2<Integer,String> item) throws Exception {
		               return item.swap();
		           }

		        });
			
			Map<String, Integer> reduceByKey = ordered.reduceByKey((i1,i2)-> i1+i2).collectAsMap();
			results.put(s, reduceByKey);
			
		}
		
		for (String s : results.keySet())
		{
			int i = 0;
			for (String s2 :results.get(s).keySet())
			{
				if (i>4)
					break;
				System.out.println(s + " mod " + s2 + " : " + results.get(s).get(s2).toString() + " times");
				i++;
			}
		}
		
//		ops.collect().forEach(t -> System.out.println(t._1 + "         " + t._2));
			
		ctx.close();
		spark.stop();
	}
}
