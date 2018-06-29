package kAnonymity2RDF;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class kAnonymityMinGen {
	
	static int rc = 3, bd = 5, gndr = 3, zp = 4;
	static int i, j, k, l;
	static float MinDistortion = Integer.MIN_VALUE;
	static int kAnonymity = 2;
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ka2rdf").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> inputData = sparkContext.textFile("tabularData");
		
		List<String> MGT = new ArrayList<>();
			
		for ( i = 0; i < rc; i++ ) {
			for (  j = 0; j < bd; j++ ) {
				for( k = 0; k < gndr; k++) {
					for( l = 0; l < zp; l++) {
						 JavaRDD<String> datas = inputData.flatMap(new FlatMapFunction<String, String>() {
								public Iterator<String> call (String line){				
									String[] temp = line.split(" ");
									List<String> result = new ArrayList<>();
									result.add(doGenRace(temp[0], i) + " " + doGenBD(temp[1], j) + " " + doGenGender(temp[2], k)+ " " + doGenZip(temp[3], l));
								return result.iterator();
							}
						 });
						 Map<String, Long> cntByValue = datas.countByValue();
						 Collection<Long> counts = cntByValue.values();
						 if( isSatisfy(kAnonymity, counts) == true ) {
							 float distortionValue = findPrec(i, j, k, l);		 
							 if ( distortionValue > MinDistortion && distortionValue < 1 ) {
								 MinDistortion = distortionValue;
								 MGT = datas.collect();
							 }
						 }
					}
				}
			}
		}
		
		JavaRDD<String> result = sparkContext.parallelize(MGT);
		System.out.println(MinDistortion);
		result.saveAsTextFile("D:/eclipse/sparkWorkspace/simplespark/result/");
		sparkContext.close();
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
}
