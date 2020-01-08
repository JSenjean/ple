package bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;

public class Ex4_1 {

	/* 
	** Fonction trouver sur StackOverflow permettant de calculer différents quartiles. On doit lui passer
	** un JavaRDD et un tableau listant les différents quartile, ces nombres sont de doubles de 0 à 1 représentant
	** les différents quartiles, pour nous, ce sera 0.25, 0.5 et 0.75 pour respectivement le premier quartile, la médiane 
	** et le troisième quartile.
	** Il permet de calculer nos trois quartiles en un seul parcours.
	*/

	public static double[] getPercentiles(JavaRDD<Double> rdd, double[] percentiles, long rddSize, int numPartitions) {
		double[] values = new double[percentiles.length];
	
		JavaRDD<Double> sorted = rdd.sortBy((Double d) -> d, true, numPartitions);
		JavaPairRDD<Long, Double> indexed = sorted.zipWithIndex().mapToPair((Tuple2<Double, Long> t) -> t.swap());
	
		for (int i = 0; i < percentiles.length; i++) {
			double percentile = percentiles[i];
			long id = (long) (rddSize * percentile);
			values[i] = indexed.lookup(id).get(0);
		}
	
		return values;
	}

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Ex1_3");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);
		Function<String, Boolean> filter = k -> (k.split(";")[4].equals("1") && !k.split(";")[3].equals("-1")
				&& !k.split(";")[0].equals("start"));

		context.close();
	}
	
}
