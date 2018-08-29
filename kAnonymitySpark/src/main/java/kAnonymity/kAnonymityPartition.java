package kAnonymity;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;
import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

 

public class kAnonymityPartition {
	
	static class stringComparator implements Serializable, Comparator<Tuple2<String, String>>{
		
		//private static finallongserialVersionUID = 1L;
		@Override
		public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
			return o1._1.compareTo(o2._1);
		}
	}
	static List<String> dataArray = new ArrayList<>();
	static int partNum = 0;
	//static String min = "";
	static Function2 func = new Function2<Integer, Iterator<Tuple2<String,String>>, List<Tuple2<String,String>>>() {
	    @Override
	    public  List<Tuple2<String,String>> call(Integer ind, Iterator<Tuple2<String,String>> s) throws Exception {
	    	List<Tuple2<String,String>> list = new ArrayList<>();
	    	if(ind == 0) {
	        	while(s.hasNext()) {            		
        			Tuple2<String,String> temp = s.next();
		    		list.add(temp);         		
	        	}
	    	}
	    	return list;
	    }
	};
	static Function2 func2 = new Function2<Integer, Iterator<Tuple2<String,String>>, Iterator<Tuple2<String,String>>>() {
	    @Override
	    public  Iterator<Tuple2<String,String>> call(Integer ind, Iterator<Tuple2<String,String>> s) throws Exception {
	    	List<Tuple2<String,String>> list = new ArrayList<>();
	    	if(ind == 1) {
	        	while(s.hasNext()) {            		
        			Tuple2<String,String> temp = s.next();
		    		list.add(temp);         		
	        	}
	    	}
	    	return list.iterator();
	    }
	};
	public static void main(String[] args) throws IOException {
		readRdf();
		
		SparkConf conf = new SparkConf().setAppName("ka2rdf").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		JavaRDD<String> rddData = sparkContext.parallelize(dataArray);
		
		JavaPairRDD<String,String> pairRDD = rddData.mapToPair(new PairFunction<String,String,String>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
			
				String[] value = s.split("-");
				String returnVal = "";
				for ( int i = 1; i < value.length; i++ ) {
					returnVal += value[i];
					if ( i + 1 != value.length )
						returnVal += "-";
				}
				
				return new Tuple2<String,String>(value[4],returnVal);
			}});
		
		pairRDD = pairRDD.sortByKey();
		System.out.println(pairRDD.count());
		
		/*int median = 0;
		if(count % 2 == 0) {
			List<Tuple2<String, String>> temp = pairRDD.take(count/2);
			List<Tuple2<String, String>> temp1 = pairRDD.take(count/2 + 1);
			Tuple2<String, String> element = temp.get(temp.size()-1);
			Tuple2<String, String> element1 = temp1.get(temp.size()-1);

			median = Integer.valueOf(element._1) + Integer.valueOf(element1._1);
			//System.out.println(median/2);
		}else {
			List<Tuple2<String, String>> temp = pairRDD.take(count/2);
			Tuple2<String, String> medianElement = temp.get(temp.size()-1);
			
			median = Integer.valueOf(medianElement._1); 
			//System.out.println(median);
			
		}*/
		
		
		pairRDD.collect();

		//System.out.println(pairRDD.collect());
		
		//pairRDD = pairRDD.partitionBy(new HashPartitioner(2));
		pairRDD = pairRDD.partitionBy(new CustomPartitioner(2));
		
		 //max = findMaxValue(pairRDD);
		 //min = findMinValue(pairRDD);
		
		JavaRDD<Tuple2<String, String>> pp = pairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,String>>, Tuple2<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<Tuple2<String,String>> iter) throws Exception {
			    
				List<Tuple2<String,String>> list = new ArrayList<>();
				List<Tuple2<String,String>> list1 = new ArrayList<>();
				iter.forEachRemaining(list1::add);
				//List list2 = IteratorUtils.toList(iter);
				String min = findMIn(list1);
				String max = findMax(list1);
			    
			    for (Tuple2<String,String> temp : list1) {
			    	list.add(new Tuple2<String,String>(temp._1,min + "-" + max));
			    }
			    /*while(iter.hasNext())
			    {  
			    	Tuple2<String,String> temp = iter.next();
			    	
			    	System.out.println(min);
			    	list.add(new Tuple2<String,String>(temp._1,min + "-" + min));
			    }*/
			    return list.iterator();
			}
		});

		
    	
		//pairRDD = pairRDD.repartition(1000);
		//System.out.println(left.collect());
		//System.out.println(right.collect());
		
		
		//Tuple2<String, String> maxVal = pairRDD.min(new stringComparator());
		//System.out.println("The max value of RDD is "+ maxVal);
		//Also create a Comparator class as below
	
		
		
		pp.saveAsTextFile("result1/");
		sparkContext.close();
	
	}
	public static String findMIn( List<Tuple2<String,String>> list) {

		String min = "";
		list.sort(new stringComparator());
		
		Tuple2<String, String> temp = list.get(0);
		min = temp._1;
		return min;
		
	}
	public static String findMax( List<Tuple2<String,String>> list) {

		String max = "";
		list.sort(new stringComparator());
		
		Tuple2<String, String> temp = list.get( list.size() - 1 );
		max = temp._1;
		return max;
		
	}
	
	public static int findMinValue(JavaPairRDD<String,String> pairRDD) {

		pairRDD = pairRDD.sortByKey();
		long count = pairRDD.count();
		List<Tuple2<String, String>> list = pairRDD.take((int) count);
		Tuple2<String, String> returnVal = list.get(0);
		return Integer.valueOf(returnVal._1);
		
	}
	public static int findMaxValue(JavaPairRDD<String,String> pairRDD) {

		pairRDD = pairRDD.sortByKey();
		long count = pairRDD.count();
		List<Tuple2<String, String>> list = pairRDD.take((int) count);
		Tuple2<String, String> returnVal = list.get(list.size() - 1);
		return Integer.valueOf(returnVal._1);
		
	}
	public static void getPart( JavaPairRDD<String,String> pairRDD, int k ) {
		
    //	if (left.count() > k ) {
    		//getPart(left, k);}
    //	if (right.count() > k) {
    		//getPart(right, k);}
	}
	
	public static void readRdf() {
			
		try { 
			   Model model = FileManager.get().loadModel("ZCTA.ttl");
			   StmtIterator iter = model.listStatements();
			   String id = "", type = "", stateCode = "", postalCode = "", census = "", zipCode = "", zipState = "", label = "";
			   Boolean hasType = false, hasStateCode = false, hasPostalCode = false, hasCensus = false, 
		   			hasZipCode = false, hasZipState = false, hasLabel = false;
			   
			   while (iter.hasNext(  )) {

				   	Statement stmt = iter.next();
				 	Resource res2 = stmt.getSubject(  );
				    Property prop = stmt.getPredicate(  );
				    RDFNode node = stmt.getObject(  );
				  
				    //System.out.println(node);
				    
				    id = res2.getLocalName().toString();
				    
				    if (prop.getLocalName().equals("label")) {
				    	label = node.toString(); 
				    	hasLabel = true;
				    } 
				    if (prop.getLocalName().equals("zipState")) {
				    	zipState = node.toString();
				    	zipState = zipState.replaceAll("http://www.census.gov/acs/pums/2014/enhanced_schema_new#", "");
				    	//System.out.println(zipState);
				    	hasZipState = true;
				    }
				    if (prop.getLocalName().equals("zipCode")) {
				    	zipCode = node.toString();
				    	hasZipCode = true;
				    }
				    if (prop.getLocalName().equals("totalPop_2010census")) {
				    
				    	census = node.toString();
				    	census = census.replaceAll("http://www.w3.org/2001/XMLSchema#integer", "");
				    	census = census.replaceAll("[\\-\\+\\.\\^:,]", "");
				    	//System.out.println(census);
				    	hasCensus = true;
				    }
				    if (prop.getLocalName().equals("statePostalCode")) {
				    	postalCode = node.toString();
				    	hasPostalCode = true;
				    }  
				    if (prop.getLocalName().equals("fipsStateCode")) {
				    	stateCode = node.toString();
				    	//stateCode = stateCode.replaceAll("http://www.w3.org/2001/XMLSchema#integer", "");
				    	//System.out.println(stateCode);
				    	hasStateCode = true;
				    }
				    if (prop.getLocalName().equals("type")) {
				    	type = node.toString();
				    	hasType = true;
				    }
				    if(hasType && hasStateCode && hasPostalCode && hasCensus && hasZipCode && hasZipState && hasLabel) {
				    	String temp = id + "-" + stateCode + "-" + postalCode + "-" + 
				    			census + "-" + zipCode + "-" + zipState;
				    	dataArray.add(temp);
				    	hasType = false; hasStateCode = false; hasPostalCode = false; hasCensus = false; 
			   			hasZipCode = false; hasZipState = false; hasLabel = false;
				    }
			   }  
			   /*int i = 1;
			   for( String s : dataArray ) {
				   System.out.println(i + " - " + s);
				   i++;
			   }*/
			} 
		 	catch (Exception e) {
		 			System.out.println("Failed: " + e);
		   }
	}
}
