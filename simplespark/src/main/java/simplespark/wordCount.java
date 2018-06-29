package simplespark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.geometry.Space;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class wordCount {
	public static void main(String[] args) throws Exception{
		/*SparkConf conf = new SparkConf().setAppName("Search").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> inputFile = sparkContext.textFile("D:/eclipse/sparkWorkspace/simplespark/test.txt");
		JavaRDD<String> words = inputFile.filter(new Function<String, Boolean>() {
		public Boolean call(String s) throws Exception {
			return s.contains("Spark");
			}});
		// count all the words
		long wordCount = words.count();
		System.out.println("word count is----" + wordCount);*/
		
		/*if(args.length < 1) {
			System.err.println("error - args");
			System.exit(1);
		}*/
		SparkConf conf = new SparkConf().setAppName("Search").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> inputFile = sparkContext.textFile("D:/eclipse/sparkWorkspace/simplespark/test.txt");
		
		JavaRDD<String> words = inputFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception{
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s){
				return new Tuple2<String, Integer>(s,1);
			}
		});
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>(){
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		
		
		System.out.println(counts.collect());
		
		//counts.saveAsTextFile("D:/eclipse/sparkWorkspace/simplespark/test1.txt");
		sparkContext.stop();
	}
}
