package itwiki.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Sparkalo {
	
	public static void main (String [] args)
	{
		
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		
		JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> lines = ctx.textFile("author", 1);
		
		JavaPairRDD<String, String> pairs = lines.mapToPair((final String s) -> {
		    String[] tokens = s.split("\t"); // cat7,234
		    return new Tuple2<String, String>(tokens[4], tokens[2]);
		});
		
		Map<String, Iterable<String>> map = pairs.groupByKey().collectAsMap();
		Map<String, Map<String,Long>> results = new HashMap<>();
		
		for (String s : map.keySet())
		{
			List<Tuple2<String, Integer>> tuples = new ArrayList<Tuple2<String,Integer>>();
			for (String s2 : map.get(s))
			{
				tuples.add(new Tuple2<String, Integer>(s2, 1));
			}
			
			JavaPairRDD<String,Integer> pairRDD = ctx.parallelizePairs(tuples);
			JavaPairRDD<String,Integer> reduceByKey = pairRDD.reduceByKey((i1,i2)-> i1+i2);
			Map<String, Long> countByKey = reduceByKey.countByKey();
			results.put(s, countByKey);
		}
		
		int i = 0;
		for (String s : results.keySet())
		{
			if (i> 4)
				break;
			for (String s2 :results.get(s).keySet())
			{
				System.out.println(s + "mod " + s2 + " : " + results.get(s).get(s2).toString() + " times");
			}
			i++;
		}
		
//		ops.collect().forEach(t -> System.out.println(t._1 + "         " + t._2));
			
		ctx.close();
		spark.stop();
	}
}
