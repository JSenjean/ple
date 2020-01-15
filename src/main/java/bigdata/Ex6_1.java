package bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

public class Ex6_1 {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Ex6_1");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);

		Function<String, Boolean> filterMultiple = k -> {
			String[] tokens = k.split(";");
			return !tokens[4].equals("0") 
				&& tokens[0].equals("start") == false;
			};

		JavaRDD<Tuple2<String,Double>> multiplePatterns = distFile.filter(filterMultiple).flatMap(l -> {
			String[] tokens = l.split(";");
			List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
			String[] patternList = tokens[3].split(",");
			for(String pattern : patternList){
				list.add(new Tuple2<String, Double>(pattern,Double.valueOf(tokens[2])));
			}
			return (list.iterator());
		});

		JavaDoubleRDD multiplePatternsDouble = multiplePatterns.mapToDouble(l -> l._2());

		Double multiplePatternTotalDuration = multiplePatternsDouble.sum();

		List<Tuple2<String,Iterable<Tuple2<String,Double>>>> multiplePatternList = multiplePatterns.groupBy(l -> l._1()).collect();

		List<String> multiplePatternsPercent = new ArrayList<>();

		multiplePatternList.forEach(l -> {
			double totalDuration = 0.0;
			for(Tuple2<String, Double> tuple : l._2()){
				totalDuration += tuple._2();
			}
			multiplePatternsPercent.add("Pourcentage du pattern " + l._1() + " : " + String.valueOf(totalDuration/multiplePatternTotalDuration * 100));
		});

		JavaRDD<String> multiplePatternRDD = context.parallelize(multiplePatternsPercent,1);

		multiplePatternRDD.saveAsTextFile("./project6_1_multiple_patterns");

		context.close();
	}
	
}

