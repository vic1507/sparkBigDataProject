package itwiki.spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


/**
 * Calcolare il numero delle pagine delle prime 5 categorie senza sottocategorie.
 * 
 * La query è eseguita sulla base di una selezione di tuple dalla tabella hive <code><u>category</u></code> ed un ordinamento.
 * 
 * @author giovannimelissari
 *
 */

public class SecondSparkQuery {
	public static void main(String[] args) {

		File out = new File("SecondQueryResults.csv");
		writeOnFile("Category\tNumOfPages", out);
		
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("Second Spark Query")
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
		
		

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
