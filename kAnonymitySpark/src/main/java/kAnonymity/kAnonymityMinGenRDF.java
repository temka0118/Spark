package kAnonymity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.jena.util.FileManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class kAnonymityMinGenRDF {
	
	static List<String> personList = new ArrayList<>();
	static int age = 3, job = 4, disease = 2;
	static int i, j, k;
	static float MinDistortion = Integer.MIN_VALUE;
	static int kAnonymity = 2;
	
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("ka2rdf").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		readRdf();
		JavaRDD<String> inputData = sparkContext.parallelize(personList);
		//System.out.println(inputData.collect());
		
		List<String> MGT = new ArrayList<String>();
		
		for ( i = 0; i < age; i++ ) {
			for (  j = 0; j < job; j++ ) {
				for( k = 0; k < disease; k++) {
					 JavaRDD<String> datas = inputData.flatMap(new FlatMapFunction<String, String>() {
							public Iterator<String> call (String line){				
								String[] temp = line.split(" ");
								List<String> result = new ArrayList<String>();
								result.add(doGenAge(temp[1], i) + " " + doGenJob(temp[2], j) + " " + doGenDisease(temp[3], k));
							return result.iterator();
						}
					 });
					 
					 /*Counting number of occurrences of each Tuples*/ 
					 Map<String, Long> cntByValue = datas.countByValue();
					 Collection<Long> counts = cntByValue.values();
					 
					 /*Checking generalizations can satisfy K value*/
					 if( isSatisfy(kAnonymity, counts) == true ) {
						 
						 /*Find distortion of generalization respect to generalized levels*/
						 float distortionValue = findPrec(i, j, k);
						 
						 if ( distortionValue > MinDistortion && distortionValue < 1 ) {
							 MinDistortion = distortionValue;
							 MGT = datas.collect();
						 }
						 break;
					 }
				}
			}
		}
		JavaRDD<String> result = sparkContext.parallelize(MGT);
		result.collect().forEach(System.out::println);
	}
	
	public static float findPrec(float i, float j, float k) {
		
		float result = 0;
		
		float r = 3;
		
		result = 1 - (( i/Float.valueOf(age) + j/Float.valueOf(job) + k/Float.valueOf(disease) ) / r );
		
		//System.out.println( result );
		
		return result;
	}
	public static boolean isSatisfy( int k, Collection<Long> counts ) {
		
		for(Long number: counts){
			if ( number < k )
				return false;
		}
		
		return true;
	}
	
	public static void readRdf() {
		
		try { 
			   Model model = FileManager.get().loadModel("Health.ttl");
			   StmtIterator iter = model.listStatements();
			   String age = "", job = "", disease = "", id ="";
			   Boolean hasAge = false, hasJob = false, hasDisease = false;
			   while (iter.hasNext(  )) {

				   	Statement stmt = iter.next();

				   	Resource res2 = stmt.getSubject(  );
				    Property prop = stmt.getPredicate(  );
				    RDFNode node = stmt.getObject(  );
				    
				    id = res2.getNameSpace().toString();
				    id = id.substring(id.length() - 4);  
				    
				    if (prop.getLocalName().equals("age")) {
				    	age = node.toString(); 
				    	hasAge = true;
				    } 
				    if (prop.getLocalName().equals("hasJob")) {
				    	job = node.toString();
				    	hasJob = true;
				    }
				    if (prop.getLocalName().equals("disease")) {
				    	disease = node.toString();
				    	hasDisease = true;
				    }
				    if(hasAge && hasJob && hasDisease) {
				    	String temp = id + " " + age + " " + job + " " + disease;
				    	personList.add(temp);
				    	hasAge = false; hasJob = false; hasDisease = false;
				    }
			   }  
			   //printGT(personList);
			} 
		 	catch (Exception e) {
			            System.out.println("Failed: " + e);
		   }
	}
	static void printGT( ArrayList<String[]> result) {
		
		for(String[] s:result) { 
			System.out.println(s[0]+ " " + s[1] + " " + s[2] + " " + s[3]);
		}
	}
	static void saveGT( ArrayList<String[]> GT) {
		
		String id = "", age = "", job = "", disease = "";
		
		String deleteQuery = "DELETE {?s ?p ?o} where { ?s ?p ?o .}";
		String insertQuery = "";

		try { 
			  UpdateRequest request = UpdateFactory.create(deleteQuery) ;
			  UpdateExecutionFactory.createRemote(request, "http://localhost:3030/health/update").execute();
			  int i = 0;
			  for ( String[] temp : GT) {
				  id = "Per" + i; i++;
				  age = temp[1];
				  job = temp[2];
				  disease = temp[3];
				  insertQuery = "prefix ex:    <ex:>" + 
							 "prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" + 
		                     "prefix foaf:  <foaf:>" + 
		                     "prefix sub:   <http://example.com/resource/Person/>" +  
		                     "INSERT DATA" + 
		                     "{" + 
		                     "sub:" + id + " foaf:age '" + age + "'; ex:job '" + job + "' ; ex:disease '" + disease + "'." +
		                     "}";
				  request = UpdateFactory.create(insertQuery) ;
				  UpdateExecutionFactory.createRemote(request, "http://localhost:3030/health/update").execute();
			  }			  
		} 
	 	catch (Exception e) {
		            System.out.println("Failed: " + e);
	   }
	}
	public static String doGenAge(String input, int gLevel) {
		String result = "";
		switch( gLevel ) {
			case 0: result = input;
				break;
			case 1: result = input.substring(0, 1) + "*";
				break;
			case 2: result = "**";
				break;
			default: break;
		}
		
		return result;
	}
	public static String doGenJob(String input, int gLevel) {
		String result = "";
		String[] professional = { "Teacher", "Lawyer", "Driver", "Engineer", "Assistant", "Baker" };
		String[] artist = { "Musician", "Writer", "Singer", "Actor", "Dancer", "Architector"};
		switch (gLevel) {
		 	case 0: result = input; 
	 			break; 
		 	case 1:	 if ( Arrays.asList(professional).contains(input) ) {
	 					 result = "Professional"; break; }
					 if ( Arrays.asList(artist).contains(input) ) {
						 result = "Artist"; break; }	
					 
			case 2: result = "Job"; break;
			default: result = "";
		}
		return result;
	}
	public static String doGenDisease(String input, int gLevel) {
		String result = "";
		switch( gLevel ) {
			case 0: result = input;
				break;
			case 1: result = "hasDisease";
				break;
			default: break;
		}
		
		return result;
	}
}
