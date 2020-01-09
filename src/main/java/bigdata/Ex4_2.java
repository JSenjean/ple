package bigdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

class SortbyValue implements Comparator<Tuple2<String,Double>>, Serializable
{ 

	/**
	 *
	 */
	private static final long serialVersionUID = -6371101274324978054L;

	@Override
	public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
		return Double.compare(o1._2(), o2._2());
	}
} 

public class Ex4_2 {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Ex4_2");
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

		JavaRDD<Tuple2<String,Double>> jobsDurations = jobsSort.map(l -> {
			double totalDuration = 0.0;
			for(Tuple2<String, Double> tuple : l._2()){
				totalDuration += tuple._2().doubleValue();
			}
			return new Tuple2<String,Double>(l._1(),Double.valueOf(totalDuration));
		});

		/*TreeMap<Double,String> jobsTree = new TreeMap<>();

		List<Tuple2<String,Double>> jobsDurationList = jobsDurations.collect();

		jobsDurationList.forEach(l -> {
			jobsTree.put(l._2(), l._1());
			if(jobsTree.size() > 10){
				jobsTree.remove(jobsTree.firstKey());
			}
		});

		List<Tuple2<String,Double>> topTenList = new ArrayList<>();

		jobsTree.entrySet().forEach(l -> {
			topTenList.add(new Tuple2<String,Double>(l.getValue(),l.getKey()));
		});*/

		List<Tuple2<String,Double>> topTenList = jobsDurations.top(10,new SortbyValue());

		JavaRDD<Tuple2<String,Double>> topTenRDD = context.parallelize(topTenList);
		topTenRDD.saveAsTextFile("./project4_2");

		context.close();
	}
	
}
