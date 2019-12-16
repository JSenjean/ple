package bigdata;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXB;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;

public class Project {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Project");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);
		Function<String, Boolean> filter = k -> (k.split(";")[4].equals("1") && !k.split(";")[3].equals("-1")
				&& !k.split(";")[0].equals("start"));

		JavaPairRDD<String,Double> cleanFile = distFile.filter(filter).mapToPair(s ->
		new Tuple2<String, Double>(s.split(";")[3],Double.valueOf(s.split(";")[2])));


		JavaPairRDD<String,Iterable<Double>> nbPatterns = cleanFile.groupByKey();

		JavaRDD<Double> variances = nbPatterns.reduce((k,v) -> v._2().iterator().next().parseDouble(s));

		JavaDoubleRDD popDouble = distFile.filter(filter).mapToDouble(k -> Double.valueOf(k.split(";")[2]));

		/*JavaRDD<Integer> npatterns = distFile.map((s) -> s.length());
		int patterns = npatterns.reduce((a, b) -> a + b);*/

		StatCounter sc = popDouble.stats();
		//System.out.println(sc.variance());
		//System.out.println(sc.max());
		// JavaRDD<String> res = String.valueOf(sc.variance());
		List<Double> l = new ArrayList<Double>();
		l.add(sc.variance());
		JavaRDD<Double> totalLengthRDD = context.parallelize(l, 1);
		totalLengthRDD.saveAsTextFile("./project2_1");

		context.close();
	}
	
}
