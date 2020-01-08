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

public class Ex1_2 {

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

		SparkConf conf = new SparkConf().setAppName("Ex1_2");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);

		/* 
		** Crée un filtre permettant d'enlever les lignes qui ne sont pas idles en détectant les -1, 
		** en vérifiant si le token 5 vaut -1
		*/
		Function<String, Boolean> filter = k -> (k.split(";")[5].equals("-1") == true
				&& k.split(";")[0].equals("start") == false);

		// On ne garde que la duration pour le transformer en JavaDoubleRDD pour récuperer des statistiques
		JavaDoubleRDD durationDouble = distFile.filter(filter).mapToDouble(k -> Double.valueOf(k.split(";")[2]));

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
		statsRDD.saveAsTextFile("./project1_2");

		JavaRDD<Tuple2<Double,Long>> histoRDD = context.parallelize(histogram,1);
		histoRDD.saveAsTextFile("./projectHisto1_2");

		context.close();
	}
	
}
