package graphX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import scala.reflect.ClassTag;
import scala.Function1;
import scala.runtime.AbstractFunction1;

public class simpleGraph {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("CustomParitioning Example").setMaster("local");
		JavaSparkContext jSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd= jSparkContext.textFile("rita2014jan.csv");
		
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		
		JavaRDD<String> airports = rdd.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call (String line){				
				String[] temp = line.split(",");
				List<String> result = new ArrayList<>();
				result.add(temp[5] + " -> " + temp[6]);
				return result.iterator();
			}
		});
		
		JavaRDD<String> routes = rdd.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call (String line){				
				String[] temp = line.split(",");
				List<String> result = new ArrayList<>();
				result.add(temp[5] + " -> " + temp[7] + " - " + temp[16]);
				return result.iterator();
			}
		});
		
		airports = airports.distinct();
		routes = routes.distinct();
		
		//Graph<String, String> graph = Graph.fromEdgeTuples(arg0, arg1, arg2, arg3);
		
		System.out.println(airports.count());
		System.out.println(routes.count());
	}
}
