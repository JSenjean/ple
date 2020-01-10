package bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

public class Ex7 {

	public static void main(String[] args) {



		SparkConf conf = new SparkConf().setAppName("Ex7");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> distFile = context.textFile(args[0]);

		if(args.length != 5){
			System.err.println("Wrong argument number");
			context.close();
			return;
		}

		List<String> patterns = new ArrayList<>();
		
		for(int i = 0; i < 4; ++i){
			patterns.add(args[i+1]);
		}


		Function<String, Boolean> filter = k -> {
			String[] tokens = k.split(";");
			if(Integer.valueOf(tokens[4]) < 4){
				return false;
			}
			String[] patternsName = tokens[3].split(",");
			return Arrays.asList(patternsName).containsAll(patterns);
		};

		JavaRDD<String> timeSlots = distFile.filter(filter).map(l -> {
			String[] tokens = l.split(";");
			return "Plage horaire : " + tokens[0] + " - " + tokens[1];
		});

		timeSlots.saveAsTextFile("./project7");

		context.close();
	}
	
}