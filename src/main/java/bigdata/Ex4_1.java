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
	 ** Fonction trouver sur StackOverflow permettant de calculer différents
	 * quartiles. On doit lui passer un JavaRDD et un tableau listant les différents
	 * quartile, ces nombres sont de doubles de 0 à 1 représentant les différents
	 * quartiles, pour nous, ce sera 0.25, 0.5 et 0.75 pour respectivement le
	 * premier quartile, la médiane et le troisième quartile. Il permet de calculer
	 * nos trois quartiles en un seul parcours.
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
		Function<String, Boolean> filter = k -> (!k.split(";")[3].equals("-1") && !k.split(";")[0].equals("start"));

		JavaRDD<Tuple2<String, Double>> splitJobs = distFile.filter(filter).flatMap(l -> {
			String[] tokens = l.split(";");
			List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();

			if (tokens[6].equals("1")) {
				list.add(new Tuple2<String, Double>(tokens[5],Double.valueOf(tokens[2])));
			}else{
				String[] jobsList = tokens[5].split(",");
				for(String job : jobsList){
					list.add(new Tuple2<String, Double>(job,Double.valueOf(tokens[2])));
				}
			}
			return (list.iterator());
		});

		JavaPairRDD<String, Iterable<Tuple2<String, Double>>> jobsSort = splitJobs.groupBy(l -> l._1());

		JavaDoubleRDD jobsDurations = jobsSort.mapToDouble(l -> {
			double totalDuration = 0.0;
			for(Tuple2<String, Double> tuple : l._2()){
				totalDuration += tuple._2().doubleValue();
			}
			return Double.valueOf(totalDuration);
		});

		// Convertit le JavaDoubleRDD en JavaRDD<Double> pour pouvoir utiliser la fonction getPercentiles
		JavaRDD<Double> jobsDurationsDouble = jobsDurations.map(k -> k);
		
		/* 
		** Récupération des statistiques avec le JavaDoubleRDD pour récuperer la moyenne, le minimum, 
		** le maximum, les quartiles et l'histogramme
		*/
		StatCounter sc = jobsDurations.stats();
		double[] diffPercentiles = {0.25,0.5,0.75};
		double[] percentiles = getPercentiles(jobsDurationsDouble, diffPercentiles, jobsDurations.count(), jobsDurations.getNumPartitions());
		double mean = sc.mean();
		double min = sc.min();
		double max = sc.max();
		Tuple2<double[],long[]> histo = jobsDurations.histogram(5);
		
		/*
		** Stocke les différentes statistiques dans une liste permettant de le convertir en JavaRDD pour pouvoir
		** l'écrire dans un fichier. 
		*/
		List<Tuple2<String,Double>> l = new ArrayList<Tuple2<String,Double>>();
		l.add(new Tuple2<String,Double>("moyenne ",mean));
		l.add(new Tuple2<String,Double>("min ",min));
		l.add(new Tuple2<String,Double>("max ",max));
		l.add(new Tuple2<String,Double>("premier quartile ",percentiles[0]));
		l.add(new Tuple2<String,Double>("mediane ",percentiles[1]));
		l.add(new Tuple2<String,Double>("troisième quartile ",percentiles[2]));

		List<Tuple2<Double,Long>> histogram = new ArrayList<Tuple2<Double,Long>>();
		for(int i = 0; i < 5; ++i){
			histogram.add(new Tuple2<Double,Long>(histo._1()[i], histo._2()[i]));
		}

		/*
		** On sépare nos résultats dans 2 fichiers, le premier contient toutes les stats d'un côté
		** et les données de l'histogramme de l'autre
		*/
		JavaRDD<Tuple2<String,Double>> statsRDD = context.parallelize(l,1);
		statsRDD.saveAsTextFile("./project1_1");

		JavaRDD<Tuple2<Double,Long>> histoRDD = context.parallelize(histogram,1);
		histoRDD.saveAsTextFile("./projectHisto1_1");

		context.close();
	}
	
}
