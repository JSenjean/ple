package bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

public class Ex5 {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Ex5");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);

		/* 
		** Crée un filtre permettant d'enlever les lignes qui sont idles en détectant les -1, en vérifiant 
		** si le token 5 ne vaut pas -1
		*/
		Function<String, Boolean> filter = k -> (k.split(";")[5].equals("-1") == true
				&& k.split(";")[0].equals("start") == false);

		// On ne garde que la duration pour le transformer en JavaDoubleRDD pour récuperer des statistiques
		JavaDoubleRDD durationDouble = distFile.filter(filter).mapToDouble(k -> Double.valueOf(k.split(";")[2]));

		Double totalDuration = durationDouble.sum();

		List<String> totalDurationString = new ArrayList<>();
		totalDurationString.add("Duree total des phases idle : " + String.valueOf(totalDuration));
		
		JavaRDD<String> totalDurationDD = context.parallelize(totalDurationString,1);
		totalDurationDD.saveAsTextFile("./project5");

		context.close();
	}
	
}

