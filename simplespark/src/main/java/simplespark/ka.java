package simplespark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class ka {
	public static void main(String[] args) {
		/*SparkConf sparkConf = new SparkConf().setAppName("ka").setMaster("local");
		// start a spark context
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		// provide path to input text file
		String path = "D:/eclipse/sparkWorkspace/simplespark/tabularData";
		
		// read text file to RDD
		JavaRDD<String> lines = sc.textFile(path);
		
		// collect RDD for printing
		
		for(String line:lines.collect()){
			String[] temp = line.split(" ");
			System.out.println(temp[0].toString());
		}
				
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception{
				return Arrays.asList(t.split(",")).iterator();
			}
		});	
			
		JavaPairRDD<String, String> keys = lines.mapToPair(s -> new Tuple2(s,s));

		Map<String, Long> result = keys.countByKey();
		
		System.out.println(result.values());*/
		
		SparkConf sparkConf = new SparkConf().setAppName("CustomParitioning Example").setMaster("local");
		JavaSparkContext jSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd= jSparkContext.textFile("tabularData");
		
		 
		JavaPairRDD<String,String> pairRDD = rdd.mapToPair(new PairFunction<String,String,String>(){

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				//return a tuple ,split[0] contains continent and split[1] contains country
				String[] value = s.split(" ");
				String returnVal = "";
				for ( int i = 0; i < value.length; i++ ) {
					returnVal += value[i];
					if ( i + 1 != value.length )
						returnVal += " ";
				}
				
				return new Tuple2<String,String>(s.split(" ")[3],returnVal);
			}});
		
		//System.out.println(pairRDD.collect());
		
		//List<String> list = pairRDD.keys().collect();
		//System.out.println(list.get(0));
		
		pairRDD = pairRDD.partitionBy(new CustomPartitioner(2));	
		//Partition part =  pairRDD.partitions().get(0);
			
		//System.out.println(pairRDD.partitions().get(0).toString());
		
		List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
        // parallelize the collection to twso partitions
        JavaRDD<Integer> rdd1 = jSparkContext.parallelize(collection,4);
        
        //System.out.println("Number of partitions : " + rdd1.getNumPartitions());
        
        JavaRDD<Integer> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
        	public Iterator<Integer> call (Integer idx, Iterator<Integer> numbers) throws Exception{
        		List<Integer> result = new ArrayList<>();
        		if(idx==1) {
        			while(numbers.hasNext()) {
        				result.add(numbers.next());
        			}
        		}
        		return result.iterator();
        	}
        },true);
        
        //System.out.println(rdd2.collect());
        
        /*rdd1.foreach(new VoidFunction<Integer>(){ 
              public void call(Integer number) {
                  System.out.println(number); 
        }});*/

		//pairRDD.saveAsTextFile("D:/eclipse/sparkWorkspace/simplespark/result/");
		
        JavaRDD<Integer> rdd3 = rdd1.mapPartitionsWithIndex((Integer idx, Iterator<Integer> numbers)->{
        	List<Integer> result = new ArrayList<>();
        	//System.out.println("test");
        	if(idx == 1) {
        		numbers.forEachRemaining(result::add);
        	}
        	return result.iterator();
        },true);
		
        //System.out.println(rdd3.collect());
        
        Function2 func = new Function2<Integer, Iterator<Tuple2<String,String>>, Iterator<String>>() {
            @Override
            public  Iterator<String> call(Integer ind, Iterator<Tuple2<String,String>> s) throws Exception {
            	List<String> list = new ArrayList<>();
            	if(ind == 1) {
	            	while(s.hasNext()) {            		
	            			Tuple2<String,String> temp = s.next();
	    		    		list.add(temp._2);         		
	            	}
            	}
            	return list.iterator();
            }
        };
        JavaRDD<String> rdd4 = pairRDD.mapPartitionsWithIndex(func, true);
        System.out.println(rdd4.collect());

	}
}
