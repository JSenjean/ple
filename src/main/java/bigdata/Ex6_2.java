package bigdata;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

public class Ex6_2 {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Ex6_2");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);

		Function<String, Boolean> filter = k -> {
			String[] tokens = k.split(";");
			return !tokens[4].equals("0") 
				&& tokens[0].equals("start") == false;
			};

		JavaRDD<Tuple2<String,Double>> allPatterns = distFile.filter(filter).flatMap(l -> {
			String[] tokens = l.split(";");
			List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
			if(tokens[4].equals("1")){
				list.add(new Tuple2<String, Double>(tokens[3],Double.valueOf(tokens[2])));
			}else{
				String[] patternList = tokens[3].split(",");
				for(String pattern : patternList){
					list.add(new Tuple2<String, Double>(pattern,Double.valueOf(tokens[2])));
				}
			}
			return (list.iterator());
		});

		JavaDoubleRDD allPatternsDouble = allPatterns.mapToDouble(l -> l._2());

		Double allPatternTotalDuration = allPatternsDouble.sum();

		List<Tuple2<String,Iterable<Tuple2<String,Double>>>> allPatternList = allPatterns.groupBy(l -> l._1()).collect();

		TreeMap<Double,String> allPatternsPercent = new TreeMap<>();

		allPatternList.forEach(l -> {
			double totalDuration = 0.0;
			for(Tuple2<String, Double> tuple : l._2()){
				totalDuration += tuple._2();
			}
			allPatternsPercent.put(totalDuration/allPatternTotalDuration * 100, l._1());
			if(allPatternsPercent.size()>10){
				allPatternsPercent.remove(allPatternsPercent.firstKey());
			}
		});

		List<String> topTenList = new ArrayList<>();

		allPatternsPercent.entrySet().forEach(l -> {
			topTenList.add("Pourcentage du pattern " + l.getValue() + " : " + String.valueOf(l.getKey()));
		});

		JavaRDD<String> topTenRDD = context.parallelize(topTenList);
		topTenRDD.saveAsTextFile("./project6_2");

		context.close();
	}
	
}

