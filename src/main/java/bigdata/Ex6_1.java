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

		Function<String, Boolean> filterMultipleOrSingle = k -> {
			String[] tokens = k.split(";");
			return !tokens[4].equals("0") 
				&& tokens[0].equals("start") == false;
			};

		JavaRDD<Tuple2<String,Double>> multipleOrSinglePatterns = distFile.filter(filterMultipleOrSingle).flatMap(l -> {
			String[] tokens = l.split(";");
			List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
			String[] patternList = tokens[3].split(",");
			for(String pattern : patternList){
				list.add(new Tuple2<String, Double>(pattern,Double.valueOf(tokens[2])));
			}
			return (list.iterator());
		});

		JavaDoubleRDD multipleOrSinglePatternsDouble = multipleOrSinglePatterns.mapToDouble(l -> l._2());

		Double multipleOrSinglePatternTotalDuration = multipleOrSinglePatternsDouble.sum();

		List<Tuple2<String,Iterable<Tuple2<String,Double>>>> multipleOrSinglePatternList = multipleOrSinglePatterns.groupBy(l -> l._1()).collect();

		List<String> multipleOrSinglePatternsPercent = new ArrayList<>();

		multipleOrSinglePatternList.forEach(l -> {
			double totalDuration = 0.0;
			for(Tuple2<String, Double> tuple : l._2()){
				totalDuration += tuple._2();
			}
			multipleOrSinglePatternsPercent.add("Pourcentage du pattern " + l._1() + " : " + String.valueOf(totalDuration/multipleOrSinglePatternTotalDuration * 100));
		});

		JavaRDD<String> multipleOrSinglePatternRDD = context.parallelize(multipleOrSinglePatternsPercent,1);

		multipleOrSinglePatternRDD.saveAsTextFile("./project6_1");

		context.close();
	}
	
}

