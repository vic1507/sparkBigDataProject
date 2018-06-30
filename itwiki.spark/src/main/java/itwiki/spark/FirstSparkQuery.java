package itwiki.spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class FirstSparkQuery {

	public static void main(String[] args) {

		File out = new File("FirstQueryResults.csv");
		writeOnFile("Autore" + "\t" + "Categoria" + "\t" + "Num modifiche", out);

		SparkSession sparkSession = SparkSession
				.builder()
				.appName("First Spark Query")
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());

		JavaPairRDD<String, String> pairs = sparkSession.sql("select * from default.lastauthor").toJavaRDD()
				.mapToPair(row -> new Tuple2<String, String>(row.get(4).toString(), row.get(2).toString()));
		//
		// JavaPairRDD<String, String> pairs = context.textFile("author", 1)
		// .mapToPair(row -> new Tuple2<String, String>(row.split("\t")[4],
		// row.split("\t")[2]));

		for (Tuple2<String, Iterable<String>> author : pairs.groupByKey().collect()) {
			List<Tuple2<String, Integer>> tuplesToCount = new ArrayList<Tuple2<String, Integer>>();
			for (String category : author._2)
				tuplesToCount.add(new Tuple2<String, Integer>(category, 1));

			context.parallelizePairs(context.parallelizePairs(tuplesToCount).reduceByKey((i1, i2) -> i1 + i2)
					.mapToPair(item -> item.swap()).sortByKey(false).take(5))
					.foreach(line -> writeOnFile(author._1 + " \t" + line._2 + "\t" + line._1, out));
		}

		context.close();
		sparkSession.stop();
	}

	static void writeOnFile(String resultsSingleRow, File out) {

		FileWriter fw;

		try {
			fw = new FileWriter(out, true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(resultsSingleRow + "\n");
			bw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
