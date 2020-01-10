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

public class Ex1_3 {

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
		Function<String, Boolean> filter = k -> {
			String[] tokens = k.split(";");
			return tokens[4].equals("1") && !tokens[3].equals("-1")
				&& !tokens[0].equals("start");
			};

		JavaRDD<Tuple2<Integer,Double>> patterns = distFile.filter(filter).map(k -> 
		new Tuple2<Integer,Double>(Integer.valueOf(k.split(";")[3]),Double.valueOf(k.split(";")[2])));

		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> patternSort = patterns.groupBy(l -> l._1());

		List<Tuple2<Integer, Iterable<Tuple2<Integer, Double>>>> patternList = patternSort.collect();

		List<String> stats = new ArrayList<>();
		List<String> histogram = new ArrayList<>();

		for (Tuple2<Integer,Iterable<Tuple2<Integer,Double>>> pattern : patternList){
			stats.add(String.valueOf("Motif numero " + pattern._1()) + " :");
			histogram.add(String.valueOf("Motif numero " + pattern._1()) + " :");

			List<Double> durations = new ArrayList<>();
			pattern._2().forEach(t -> 
			durations.add(t._2())
			);

			JavaDoubleRDD durationDouble = context.parallelizeDoubles(durations);

			// Convertit le JavaDoubleRDD en JavaRDD<Double> pour pouvoir utiliser la fonction getPercentiles
			JavaRDD<Double> durationRDDDouble = durationDouble.map(k -> k);
			
			/* 
			** Récupération des statistiques avec le JavaDoubleRDD pour récuperer la moyenne, le minimum, 
			** le maximum, les quartiles et l'histogramme
			*/
			StatCounter sc = durationDouble.stats();
			double[] diffPercentiles = {0.25,0.5,0.75};
			double[] percentiles = getPercentiles(durationRDDDouble, diffPercentiles, durationDouble.count(), durationDouble.getNumPartitions());
			double mean = sc.mean();
			double min = sc.min();
			double max = sc.max();
			Tuple2<double[],long[]> histo = durationDouble.histogram(5);
			
			/*
			** Stocke les différentes statistiques dans une liste permettant de le convertir en JavaRDD pour pouvoir
			** l'écrire dans un fichier. 
			*/
			stats.add("moyenne " + String.valueOf(mean));
			stats.add("min " + String.valueOf(min));
			stats.add("max " + String.valueOf(max));
			stats.add("premier quartile " + String.valueOf(percentiles[0]));
			stats.add("mediane " + String.valueOf(percentiles[1]));
			stats.add("troisième quartile " + String.valueOf(percentiles[2]));

			for(int i = 0; i < 5; ++i){
				histogram.add(String.valueOf(histo._1()[i]) + " : " + String.valueOf(histo._2()[i]));
			}

		}

		/*
		** On sépare nos résultats dans 2 fichiers, le premier contient toutes les stats d'un côté
		** et les données de l'histogramme de l'autre
		*/
		JavaRDD<String> statsRDD = context.parallelize(stats,1);
		statsRDD.saveAsTextFile("./project1_3");

		JavaRDD<String> histoRDD = context.parallelize(histogram,1);
		histoRDD.saveAsTextFile("./projectHisto1_3");

		context.close();
	}
	
}
