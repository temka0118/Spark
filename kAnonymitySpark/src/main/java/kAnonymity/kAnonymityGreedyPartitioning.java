package kAnonymity;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.sparql.function.library.min;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class kAnonymityGreedyPartitioning {
	static class integerComparator implements Comparator<Integer>,Serializable{
		
		@Override
		  public int compare(Integer a, Integer b) {
		    return a.compareTo(b);
		  }
	}

	static class stringComparator implements Serializable, Comparator<String>{
		
		//private static finallongserialVersionUID = 1L;
		@Override
		public int compare(String s1, String s2) {
			return s1.compareTo(s2);
		}
	}
	static List<Integer> medians = new ArrayList<>();
	
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("kaGreedyPartitioning").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> inputData = sparkContext.textFile("testData");
		
		JavaPairRDD<String,String> pairRDD = inputData.mapToPair(new PairFunction<String,String,String>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
			
				String[] value = s.split(" ");
				String returnVal = "";
				for ( int i = 0; i < value.length; i++ ) {
					returnVal += value[i];
					if ( i + 1 != value.length )
						returnVal += " ";
				}
				
				return new Tuple2<String,String>(value[2],returnVal);
		}});
		
		//pairRDD.collect().forEach(System.out::println);
			
		isAllowableCut(pairRDD, 2);
	
		medians = distinct(medians);
		medians.sort(new integerComparator());
		//System.out.println(medians);
	
		int numParts = medians.size() + 1;
		
		pairRDD = pairRDD.partitionBy(new GreedyPartitioner(numParts,medians));
		
		JavaRDD<String> result = pairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,String>>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Iterator<Tuple2<String,String>> iter) throws Exception {
			    
				List<String> returnList = new ArrayList<>();
				List<Tuple2<String,String>> inputList = new ArrayList<>();
				iter.forEachRemaining(inputList::add);
			    
				returnList = doAnonymize(inputList);
  
			    return returnList.iterator();
			}
		});
		
		
		result = result.coalesce(1);
		//result.collect();
		
		result.saveAsTextFile("result/");
		
		sparkContext.close();
		
		
	}
	public static List<String> doAnonymize(List<Tuple2<String,String>> inputList){
		
		String maxAge = "", minAge = "" , maxZipCode = "", minZipCode = "", gender = "";
		Boolean isAge = false, isGender = false, isZipCode = false; 
		List<String> ages = new ArrayList<>();
		List<String> genders = new ArrayList<>();
		List<String> zipCodes = new ArrayList<>();
		List<String> Diseases = new ArrayList<>();
		List<String> returnList = new ArrayList<>();
		
		for( Tuple2<String,String> temp : inputList ) {
			String QIDs[] = temp._2.split(" ");
			ages.add(QIDs[0]);
			genders.add(QIDs[1]);
			zipCodes.add(QIDs[2]);
			Diseases.add(QIDs[3]);
		}
		if(distinctCount(ages) > 1){
			ages.sort(new stringComparator());
			maxAge = ages.get(ages.size() - 1); minAge = ages.get(0);
			isAge = true;
		}
		if(distinctCount(genders) > 1) {
			gender = "Human";
			isGender = true;
		}
		if(distinctCount(zipCodes) > 1) {
			zipCodes.sort(new stringComparator());
			maxZipCode = zipCodes.get(zipCodes.size() - 1); minZipCode = zipCodes.get(0);
			isZipCode = true;
		}
		
		for( int i = 0; i < Diseases.size(); i++ ) {
			String temp = "";
			if ( isAge ) {
				temp = temp + minAge + "-" + maxAge + " "; 
			}else {
				temp += ages.get(i) + " ";
			}
			if ( isGender ) {
				temp = temp + gender + " "; 
			}else {
				temp += genders.get(i) + " ";
			}
			if ( isZipCode ) {
				temp = temp + minZipCode + "-" + maxZipCode + " "; 
			}else {
				temp += zipCodes.get(i) + " ";
			}
			temp += Diseases.get(i);
			returnList.add(temp);
		}
		
		return returnList;
	}
	public static List<Integer> distinct(List<Integer> list) {
		List<Integer> returnList = new ArrayList<>();
		
		int i = 0;
		for (i = 0; i < list.size(); i++) {
			if( !returnList.contains(list.get(i))) {
				returnList.add(list.get(i));
			}
		}
		return returnList;
	}
	public static int distinctCount(List<String> list) {
		int i = 0, count = 0;
		for (i = 0, count = 1 ; i < list.size() - 1; i++) {
			if( !list.get(i).equals(list.get(i + 1) ) ) {
				count++;
			}
		}
		return count;
	}
	public static int isAllowableCut(JavaPairRDD<String,String> pairRDD, int k) {
		
		if( pairRDD.count() <= k )
			return 0;
		
		int median = findMedian(pairRDD);
		
		
		JavaPairRDD<String, String> leftPart = pairRDD.filter((x -> (Integer.valueOf(x._1) <= median)));
		JavaPairRDD<String, String> rightPart = pairRDD.filter((x -> (Integer.valueOf(x._1) > median)));


		
		if ( leftPart.count() >= k && rightPart.count() >= k ) {
			medians.add(median);
			isAllowableCut(leftPart, k);
			isAllowableCut(rightPart, k);
		}
		
		return 1;
	}
	public static int findMedian(JavaPairRDD<String,String> pairRDD) {
		
		pairRDD = pairRDD.sortByKey();
		int median = 0;
		int count = (int) pairRDD.count();
		if(count % 2 == 0) {
			
			List<Tuple2<String, String>> temp = pairRDD.take(count/2);
			List<Tuple2<String, String>> temp1 = pairRDD.take(count/2 + 1);
			Tuple2<String, String> element = temp.get(temp.size()-1);
			Tuple2<String, String> element1 = temp1.get(temp.size()-1);

			median = (Integer.valueOf(element._1) + Integer.valueOf(element1._1))/2;
			System.out.println(median);
		}else {
			
			List<Tuple2<String, String>> temp = pairRDD.take(count/2 + 1);
			Tuple2<String, String> medianElement = temp.get(temp.size()-1);
			
			median = Integer.valueOf(medianElement._1); 
			System.out.println(median);
		}
		
		return median;
	}
}
