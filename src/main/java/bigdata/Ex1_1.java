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

public class Ex1_1 {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Ex1_1");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);
		Function<String, Boolean> filter = k -> (k.split(";")[5].equals("-1") == false
				&& k.split(";")[0].equals("start") == false);

		// JavaPairRDD<String,String> cleanFile = distFile.filter(filter).mapToPair(s ->
		// new Tuple2<String, String>(s.split(";")[1],s.split(";")[4]));
		JavaDoubleRDD popDouble = distFile.filter(filter).mapToDouble(k -> Double.valueOf(k.split(";")[2]));

		StatCounter sc = popDouble.stats();
		//System.out.println(sc.variance());
		//System.out.println(sc.max());
		// JavaRDD<String> res = String.valueOf(sc.variance());
		List<Double> l = new ArrayList<Double>();
		l.add(sc.variance());
		JavaRDD<Double> totalLengthRDD = context.parallelize(l, 1);
		totalLengthRDD.saveAsTextFile("./project1_1");

		context.close();
	}
	
}

