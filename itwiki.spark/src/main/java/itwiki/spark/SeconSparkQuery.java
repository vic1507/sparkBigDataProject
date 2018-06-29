package itwiki.spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


/**
 * Calcolare il numero delle pagine delle prime 5 categorie senza sottocategorie.
 * 
 * @author giovannimelissari
 *
 */

public class SeconSparkQuery {
	public static void main(String[] args) {

		File out = new File("SecondQueryResults.csv");
		writeOnFile("Category" +"\t" + "NumOfPages", out);
		
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("Simple Application")
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
		
		writeOnFile("Category\tNumOfPages", out);

		// le categorie che non hanno sottocategorie
//		JavaPairRDD<String, String> leafCategories = sparkSession.sql("select default.page.page_id as pageid, distinct(default.page.page_title) as categorytitle \n" + 
//															"from default.categorylinks, default.page \n" + 
//															"WHERE default.cl_type=\"subcat\" AND  \n" + 
//															"	default.cl_from = default.page.page_id\n" + 
//															"    and default.page.page_title not in \n" + 
//															"    (select default.cl.cl_to from default.categorylinks as cl where cl.cl_type=\"subcat\")")
//										.toJavaRDD()
//										.mapToPair(row -> new Tuple2<String, String>(row.get(1).toString(), row.get(0).toString()));
		
		
//		// le pagine con le categorie da categoryrebuilt
//		JavaPairRDD<String, String> categoryRebuilt = sparkSession.sql("select * from default.categoryrebuilt")
//										.toJavaRDD()
//										.mapToPair(row -> new Tuple2<String, String>(row.get(2).toString(), row.get(1).toString()));
		
//		
//		JavaPairRDD<String, Tuple2<String, Optional<String>>> leftOuterJoin = leafCategories.leftOuterJoin(categoryRebuilt);
//		
//		leftOuterJoin.collect().forEach(item -> System.out.println(item._1 + ", " + item._2._2.toString()));
		

		// cateogorie che sono foglie con il numero di pagine
		JavaPairRDD<Integer, String> category = sparkSession.sql("select * from default.category where cat_subcats = 0")
				.toJavaRDD()
				.mapToPair(row -> new Tuple2<Integer, String>(row.getInt(2), row.get(1).toString()));
		
		// ordina dal più grande al più piccolo
		category.sortByKey(false).take(5).forEach(c -> writeOnFile(c._2 + "\t" + c._1, out));

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
