package itwiki.spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


/**
 * Calcolare il numero delle pagine delle prime 5 categorie senza sottocategorie.
 * 
 * La query è eseguita sulla base di una computazione fatta sulle seguenti tabelle hive:
 * <ul>
 * <li> categorylinks </li>
 * <li> categoryrebuilt </li>
 * </ul>
 * @author giovannimelissari
 *
 */
public class SecondSparkQueryAdv {
	
	
	public static void main(String[] args) {

		File out = new File("SecondQueryResultsAdv.csv");
		writeOnFile("Category" +"\t" + "NumOfPages", out);
		
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("Second Spark Query Advanced")
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());

		// le categorie che non hanno sottocategorie. Struttura <categoria: string, id_categoria: string>
		JavaPairRDD<String, String> leafCategories = sparkSession.sql("SELECT DISTINCT page.page_id as pageid, page.page_title AS categorytitle " + 
															"FROM default.categorylinks, default.page " + 
															"WHERE cl_type=\"subcat\" " + 
															"	 AND cl_from = page.page_id" + 
															"    AND page.page_title " + 
															"    NOT IN " +
															"    (SELECT cl.cl_to FROM default.categorylinks AS cl WHERE cl.cl_type=\"subcat\")")
										.toJavaRDD()
										.mapToPair(row -> new Tuple2<String, String>(row.get(1).toString(), row.get(0).toString()));
		
//		// le pagine con le categorie da categoryrebuilt. Struttura <categoria: string, pagina: string>
		JavaPairRDD<String, String> categoryRebuilt = sparkSession.sql("SELECT * FROM default.categoryrebuilt")
										.toJavaRDD()
										.mapToPair(row -> new Tuple2<String, String>(row.get(2).toString(), row.get(1).toString()));
		
		
		// inner join
		JavaPairRDD<String, Tuple2<String, String>> join = categoryRebuilt.join(leafCategories);
		
		
		// count del numero di pagine raggruppate per categoria
		List<Tuple2<String, Integer>> tuples = new ArrayList<Tuple2<String, Integer>>();
		for (Tuple2<String, Iterable<Tuple2<String, String>>> category : join.groupByKey().collect()) {
			
			int numOfPages = 0;
			if(category._2 instanceof Collection<?>)
				numOfPages = ((Collection<?>)category._2).size();
			
			tuples.add(new Tuple2<String, Integer>(category._1, numOfPages));
		}
		
		// ordino per valore (numero di pagina), prendo le prime cinque categorie e stampo su file
		context.parallelizePairs(tuples)
		.mapToPair(item -> item.swap()).sortByKey(false).take(5)
		.forEach(tuple -> writeOnFile(tuple._2 + "\t" + tuple._1, out));


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
