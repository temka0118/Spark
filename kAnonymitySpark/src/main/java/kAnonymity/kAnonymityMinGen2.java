package kAnonymity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;

public class kAnonymityMinGen2 {
	
	/*Declaring variable respect to Domain Generalization Hierarchies values*/
	static int rc = 3, bd = 5, gndr = 3, zp = 4;
	static int i, j, k, l;
	static float MinDistortion = Integer.MIN_VALUE;
	static int kAnonymity = 2;
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ka2rdf").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> inputData = sparkContext.textFile("tabularData");
		
		JavaRDD<String> race = inputData.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")[0]).iterator();
			}
		});
		JavaRDD<String> birthDate = inputData.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")[1]).iterator();
			}
		});
		
		
		JavaPairRDD<String, String> races = inputData.mapToPair(new PairFunction<String, String, String>() {
		    public Tuple2<String, String> call(String s) throws Exception {
		    	String key = splitSpaces(s);
		    	return new Tuple2<String, String>(key, s.split(" ")[0]);
		}});
		JavaPairRDD<String, String> birthDates = inputData.mapToPair(new PairFunction<String, String, String>() {
		    public Tuple2<String, String> call(String s) throws Exception {
		    	String key = splitSpaces(s);
		    	return new Tuple2<String, String>(key, s.split(" ")[1]);
		}});
		JavaPairRDD<String, String> genders = inputData.mapToPair(new PairFunction<String, String, String>() {
		    public Tuple2<String, String> call(String s) throws Exception {
		    	String key = splitSpaces(s);
		    	return new Tuple2<String, String>(key, s.split(" ")[2]);
		}});
		JavaPairRDD<String, String> zipCodes = inputData.mapToPair(new PairFunction<String, String, String>() {
		    public Tuple2<String, String> call(String s) throws Exception {
		    	String key = splitSpaces(s);
		    	return new Tuple2<String, String>(key, s.split(" ")[3]);
		}});
		
		JavaPairRDD<String, String> joins = races.union(birthDates).union(genders).union(zipCodes);
		
	
		//System.out.println(joins);
		//JavaPairRDD<String, Iterable<String>> join = joins.groupByKey();
		
	
		JavaPairRDD<String, String> join = joins.reduceByKey(new Function2<String, String, String>() {
			public String call(String v1, String v2) throws Exception {
				return v1 + " " + v2;
		}});
		
		Map<String, Long> d = joins.countByKey();
		
		System.out.println(d);
		
		JavaPairRDD<String, List<String>> races1 = inputData.mapToPair(new PairFunction<String, String, List<String>>() {
		    public Tuple2<String, List<String>> call(String s) throws Exception {
		    	String key = splitSpaces(s);
		    	return new Tuple2<String, List<String>>(key, allGensRace(s.split(" ")[0]));
		}});
		
		JavaPairRDD<String, List<String>> birthDates1 = inputData.mapToPair(new PairFunction<String, String, List<String>>() {
		    public Tuple2<String, List<String>> call(String s) throws Exception {
		    	String key = splitSpaces(s);
		    	return new Tuple2<String, List<String>>(key, allGensBD(s.split(" ")[1]));
		}});
		


		//System.out.println(birthDates1.collect());
		//System.out.println(join.collect());
		 
		sparkContext.close();
	}
	public static String splitSpaces(String s) {
		
		String[] words = s.split(" ");
		String result = "";
		for(String word : words) {
			result += word;
		}		
		
		return result;
	}
	public static float findPrec(float i, float j, float k, float l) {
		
		float result = 0;
		
		float r = 4;
		
		result = 1 - (( i/Float.valueOf(rc) + j/Float.valueOf(bd) + k/Float.valueOf(gndr) + l/Float.valueOf(zp) ) / r );
		
		System.out.println( result );
		
		return result;
	}
	public static boolean isSatisfy( int k, Collection<Long> counts ) {
		
		for(Long number: counts){
			if ( number < k )
				return false;
		}
		
		return true;
	}
	public static String doGenRace(String input, int gLevel) {
		String result = "";
		switch( gLevel ) {
			case 0: result = input;
				break;
			case 1: result = "Person";
				break;
			case 2: result = "*****";
				break;
			default: break;
		}
		
		return result;
	}
	public static String doGenBD(String input, int gLevel) {
		String result = "";
		switch( gLevel ) {
			case 0: result = input;
				break;
			case 1: result = input.substring(input.length() - 4, input.length());
				break;
			case 2: result = input.substring(input.length() - 4, input.length());
				    result = (Integer.valueOf(result) > 1964) ? "1965-1969" : "1960-1964";
				break;
			case 3: result = "1960-1969";
				break;
			case 4: result = "*****";
				break;
			default: break;
		}
		return result;
	}
	public static String doGenGender(String input, int gLevel) {
		String result = "";
		switch( gLevel ) {
			case 0: result = input;
				break;
			case 1: result = "Human";
				break;
			case 2: result = "*****";
				break;
			default: break;
			}
		return result;
	}
	public static String doGenZip(String input, int gLevel) {
		String result = "";
		switch( gLevel ) {
			case 0: result = input;
				break;
			case 1: result = input.substring(0,  input.length() - 1) + "*";
				break;
			case 2: result = input.substring(0,  input.length() - 2) + "**";
				break;
			case 3: result = "*****";
				break;
			default: break;
		}
		return result;
	}
	public static List<String> allGensRace(String s) {
		
		List<String> values = new ArrayList<>();
		
			values.add(doGenRace(s,0));
			values.add(doGenRace(s,1));
			values.add(doGenRace(s,2));
			
		return values;
	}
	public static List<String> allGensBD(String s) {
		
		List<String> values = new ArrayList<>();
		
			values.add(doGenBD(s,0));
			values.add(doGenBD(s,1));
			values.add(doGenBD(s,2));
			values.add(doGenBD(s,3));
			
		return values;
	}
	
}
