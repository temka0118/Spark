package kAnonymity;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.function.library.min;
import org.apache.jena.util.FileManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import kAnonymity.kAnonymityGreedyPartitioning.integerComparator;
import kAnonymity.kAnonymityGreedyPartitioning.stringComparator;
import scala.Tuple2;

public class kAnonymityGreedyPartitioningLUBM {
	
	static class integerComparator implements Comparator<Integer>,Serializable{
		
		private static final long serialVersionUID = 1L;

		@Override
		  public int compare(Integer a, Integer b) {
		    return a.compareTo(b);
		  }
	}

	static class stringComparator implements Serializable, Comparator<String>{
		
		private static final long serialVersionUID = 1L;

		//private static finallongserialVersionUID = 1L;
		@Override
		public int compare(String s1, String s2) {
			return s1.compareTo(s2);
		}
	}
	static List<Integer> medians = new ArrayList<>();
	static List<String> arrayList = new ArrayList<>();
	
	public static void main(String[] args) throws IOException {
		
		SparkConf conf = new SparkConf().setAppName("kaGreedyPartitioning").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		readRdf();
		JavaRDD<String> inputData = sparkContext.parallelize(arrayList);

		/* JavaRDD<String> research = inputData.flatMap(new FlatMapFunction<String, String>() {
				public Iterator<String> call (String line){				
					String[] temp = line.split(" ");
					List<String> result = new ArrayList<String>();
					result.add(temp[1]);
				return result.iterator();
			}
		 }).distinct();
		 JavaRDD<String> worksfor = inputData.flatMap(new FlatMapFunction<String, String>() {
				public Iterator<String> call (String line){				
					String[] temp = line.split(" ");
					List<String> result = new ArrayList<String>();
					result.add(temp[2]);
				return result.iterator();
			}
		 }).distinct();
		 JavaRDD<String> bachelor = inputData.flatMap(new FlatMapFunction<String, String>() {
				public Iterator<String> call (String line){				
					String[] temp = line.split(" ");
					List<String> result = new ArrayList<String>();
					result.add(temp[3]);
				return result.iterator();
			}
		 }).distinct();
		 JavaRDD<String> master = inputData.flatMap(new FlatMapFunction<String, String>() {
				public Iterator<String> call (String line){				
					String[] temp = line.split(" ");
					List<String> result = new ArrayList<String>();
					result.add(temp[4]);
				return result.iterator();
			}
		 }).distinct();
		 JavaRDD<String> doctor = inputData.flatMap(new FlatMapFunction<String, String>() {
				public Iterator<String> call (String line){				
					String[] temp = line.split(" ");
					List<String> result = new ArrayList<String>();
					result.add(temp[5]);
				return result.iterator();
			}
		 }).distinct();*/
		 
		 
		 
		/*System.out.println(research.count());
		 System.out.println(worksfor.count());
		 System.out.println(bachelor.count());
		 System.out.println(master.count());
		 System.out.println(doctor.count());*/
		
		JavaPairRDD<Integer,String> pairRDD = inputData.mapToPair(new PairFunction<String,Integer,String>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, String> call(String s) throws Exception {
			
				String[] value = s.split(" ");
				String returnVal = "";
				for ( int i = 0; i < value.length; i++ ) {
					returnVal += value[i];
					if ( i + 1 != value.length )
						returnVal += " ";
				}

				return new Tuple2<Integer,String>(getIntValue(value[4]),returnVal);
		}});
		
		//pairRDD = pairRDD.sortByKey();
		//pairRDD.collect().forEach(System.out::println);
		//System.out.println(pairRDD.count());
		List<Integer> dimensionVals = new ArrayList<>();
		
		for(String line : arrayList) {
			String[] values = line.split(" ");
			dimensionVals.add(getIntValue(values[4]));
		}

		isAllowableCut(dimensionVals, 2);
				
		medians = distinct(medians);
		medians.sort(new integerComparator());
		
		int numParts = medians.size() + 1;
		
		pairRDD = pairRDD.partitionBy(new GreedyPartitioner(numParts,medians));
		
		JavaRDD<String> result = pairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer,String>>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Iterator<Tuple2<Integer,String>> iter) throws Exception {
			    
				List<String> returnList = new ArrayList<>();
				List<Tuple2<Integer,String>> inputList = new ArrayList<>();
				iter.forEachRemaining(inputList::add);
			    
				returnList = doAnonymize(inputList);
  
			    return returnList.iterator();
			}
		});
		
		//result = result.coalesce(1);
		result.saveAsTextFile("result/");
		
		//medians.forEach(System.out::println);
		
		//System.out.println(pairRDD.count());
		sparkContext.close();
	}
	public static List<String> doAnonymize(List<Tuple2<Integer,String>> inputList){
		
		String minResearch = "Research", maxResearch = "";
		String minWorksFor = "<http://www.Department", maxWorksFor = ""; 
		String minUniversity = "", maxUniversity = ""; 
		String minBachelors = "<http://www.University", maxBachelors = ""; 
		String minMasters = "<http://www.University", maxMasters = ""; 
		String minDoctors = "<http://www.University", maxDoctors = ""; 
		Boolean isResearch = false, isBachelor = false, isMaster = false, isDoctor = false, isWorksFor = false;
		List<String> research = new ArrayList<>();
		List<String> worksFor = new ArrayList<>();
		List<String> bachelors = new ArrayList<>();
		List<String> masters = new ArrayList<>();
		List<String> doctors = new ArrayList<>();
		
		List<String> returnList = new ArrayList<>();
		
		for( Tuple2<Integer,String> temp : inputList ) {
			String QIDs[] = temp._2.split(" ");
			research.add(QIDs[1]);
			worksFor.add(QIDs[2]);
			bachelors.add(QIDs[3]);
			masters.add(QIDs[4]);
			doctors.add(QIDs[5]);
		}
		if(distinctCount(research) > 1){
			List<Integer> temp = getIntValueArray(research);
			temp.sort(new integerComparator());
			maxResearch += temp.get(temp.size() - 1); minResearch += temp.get(0);
			isResearch = true;			
		}
		if(distinctCount(worksFor) > 1) {
			List<Integer> temp = getIntValueArray(worksFor);
			temp.sort(new integerComparator());
			
			List<Integer> temp1 = getIntValueArray(worksFor,1);
			temp1.sort(new integerComparator());
			
			maxWorksFor += temp.get(temp.size() - 1); minWorksFor += temp.get(0);
			maxUniversity += temp1.get(temp1.size() - 1); minUniversity += temp1.get(0);
			isWorksFor = true;
		}
		if(distinctCount(bachelors) > 1) {
			List<Integer> temp = getIntValueArray(bachelors);
			temp.sort(new integerComparator());
			maxBachelors += temp.get(research.size() - 1); minBachelors += temp.get(0);
			isBachelor = true;
		}
		if(distinctCount(masters) > 1) {
			List<Integer> temp  = getIntValueArray(masters);
			temp.sort(new integerComparator());	
			maxMasters += temp.get(temp.size() - 1); minMasters += temp.get(0);
			isMaster = true;
		}
		if(distinctCount(doctors) > 1) {
			List<Integer> temp  = getIntValueArray(doctors);
			temp.sort(new integerComparator());
			maxDoctors += temp.get(temp.size() - 1); minDoctors += temp.get(0);
			isDoctor = true;
		}
		
		for( int i = 0; i < inputList.size(); i++ ) {
			String temp = "";
			if ( isResearch ) {
				temp = temp + minResearch + "-" + maxResearch + " "; 
			}else {
				temp += research.get(i) + " ";
			}
			if ( isWorksFor ) {
				temp += minWorksFor + "-" + maxWorksFor + ".";
				if (Integer.valueOf(minUniversity) == Integer.valueOf(maxUniversity)){
					temp += "University" + minUniversity + ".edu "; 
				}else {
					temp += "University" + minUniversity + "-" + maxUniversity + ".edu ";
				}
			}else {
				temp += worksFor.get(i) + " ";
			}
			if ( isBachelor ) {
				temp = temp + minBachelors + "-" + maxBachelors + ".edu "; 
			}else {
				temp += bachelors.get(i) + " ";
			}
			if ( isMaster ) {
				temp = temp + minMasters + "-" + maxMasters + ".edu "; 
			}else {
				temp += masters.get(i) + " ";
			}
			if ( isDoctor ) {
				temp = temp + minDoctors + "-" + maxDoctors + ".edu"; 
			}else {
				temp += doctors.get(i);
			}
			returnList.add(temp);
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
	public static int getIntValue(String s) {
		try {
			if(s.contains("Department")) {
				//s = s.replaceAll("University", "");
				s = s.substring(0, 26);
			}
			int returnVal = Integer.parseInt(s.replaceAll("\\D", ""));
			 //System.out.println(returnVal);
			return returnVal;
		}catch (Exception e) {
            System.out.println("Failed: " + e);
		}
		return 0;
	}
	public static int getIntValue(String s, int i) {
		try {
			if(s.contains("Department")) {
				s = s.substring(26, s.length());
			}
			int returnVal = Integer.parseInt(s.replaceAll("\\D", ""));
			 //System.out.println(returnVal);
			return returnVal;
		}catch (Exception e) {
            System.out.println("Failed: " + e);
		}
		return 0;
	}
	public static List<Integer> getIntValueArray(List<String> list) {
		List<Integer> returnList = new ArrayList<>();
		int i = 0;
		for (i = 0; i < list.size(); i++) {
			returnList.add(getIntValue(list.get(i)));
		}
		return returnList;
	}
	public static List<Integer> getIntValueArray(List<String> list, int l) {
		List<Integer> returnList = new ArrayList<>();
		int i = 0;
		for (i = 0; i < list.size(); i++) {
			returnList.add(getIntValue(list.get(i), 1));
		}
		return returnList;
	}
	public static int isAllowableCut(List<Integer> list, int k) {
		
		if( list.size() <= k )
			return 0;
		
		int median = findMedian(list);
		
		List<Integer> leftPart = findLeft(list, median);
		List<Integer> rightPart = findRight(list, median);

		//System.out.println(leftPart.count() + "-" + rightPart.count() + "-" + median);
		if ( leftPart.size() >= k && rightPart.size() >= k ) {
			medians.add(median);
			isAllowableCut(leftPart, k);
			isAllowableCut(rightPart, k);
		}
		
		return 1;
	}
	public static List<Integer> findLeft(List<Integer> list, int median ) {
		
		List<Integer> returnList = new ArrayList<>();
		for (Integer val : list) {
			if ( val <= median)
				returnList.add(val);
		}
		return returnList;
	
	}
	public static List<Integer> findRight(List<Integer> list, int median ) {
		
		List<Integer> returnList = new ArrayList<>();
		for (Integer val : list) {
			if ( val > median)
				returnList.add(val);
		}
		return returnList;
	
	}
	public static int findMedian(List<Integer> list) {
		
		list.sort(new integerComparator());
		
		//System.out.println(list);
		
		int median = 0;
		int count = list.size();
		
		if(count % 2 == 0) {
			Integer temp = list.get(count/2 - 1);
			Integer temp1 = list.get(count/2);
			median = (temp + temp1)/2;
			//System.out.println(median);
		}else {
			median =  list.get(count/2);
			//System.out.println(median);
		}
		
		return median;
	}
	public static void readRdf() {
		
		try { 
			   Model model = FileManager.get().loadModel("Universities-10.ttl");
			   StmtIterator iter = model.listStatements();
			   String name = ""; String emailAddress = ""; String telephone = ""; String type = ""; String researchInterest = ""; 
			   String worksFor = "";   String teacherOf = ""; String undergraduateDegreeFrom = "";
			   String mastersDegreeFrom = ""; String doctoralDegreeFrom = "";
			   
			   Boolean hasName = false; Boolean hasEmail = false; Boolean hasTel = false; Boolean hasType = false; Boolean hasRsrch = false;
			   Boolean hasWork = false; Boolean hasTeach = false; Boolean hasUnderDeg = false; Boolean hasMasDeg = false; Boolean hasDoc = false;
			   
			   
			   while (iter.hasNext(  )) {

				   	Statement stmt = iter.next();
				   	
				   	Resource res2 = stmt.getSubject(  );
				    Property prop = stmt.getPredicate(  );
				    RDFNode node = stmt.getObject(  );
				    if (res2.getLocalName().contains("FullProfessor")||res2.getLocalName().contains("AssociateProfessor")||res2.getLocalName().contains("AssistantProfessor")) {
				    	//System.out.print(res.getLocalName() + " - ");
				    	//System.out.print(prop.getLocalName() + " - ");
				    	//System.out.println(node);
				    	if( prop.getLocalName().equals("name") ) {
				    		name = node.toString();
				    		hasName = true;
				    	}
				    	if( prop.getLocalName().equals("emailAddress") ) {
				    		emailAddress = node.toString();
				    		hasEmail = true;
				    	}
				    	if( prop.getLocalName().equals("telephone") ) {
				    		telephone = node.toString();
				    		hasTel = true;
				    	}
				    	if( prop.getLocalName().equals("type") ) {
				    		type = node.toString();
				    		hasType = true;
				    	}
				    	if( prop.getLocalName().equals("researchInterest") ) {
				    		researchInterest = node.toString();
				    		hasRsrch = true;
				    	}
				    	if( prop.getLocalName().equals("worksFor") ) {
				    		worksFor = node.toString();
				    		hasWork = true;
				    	}
				    	if( prop.getLocalName().equals("teacherOf") ) {
				    		teacherOf = node.toString();
				    		hasTeach = true;
				    	}
				    	if( prop.getLocalName().equals("undergraduateDegreeFrom") ) {
				    		undergraduateDegreeFrom = node.toString();
				    		hasUnderDeg = true;
				    	}
				    	if( prop.getLocalName().equals("mastersDegreeFrom") ) {
				    		mastersDegreeFrom = node.toString();
				    		hasMasDeg = true;
				    	}
				    	if( prop.getLocalName().equals("doctoralDegreeFrom") ) {
				    		doctoralDegreeFrom = node.toString();
				    		hasDoc = true;
				    	}
				    	if ( hasName && hasEmail && hasTel && hasType && hasRsrch && hasWork && hasTeach && hasUnderDeg && hasMasDeg && hasDoc ) {
				    		
				    		String temp = name + " " + researchInterest + " " + worksFor + " " + undergraduateDegreeFrom + " " + mastersDegreeFrom + " " + doctoralDegreeFrom;
					    	arrayList.add(temp);
				    		hasName = false; hasEmail = false; hasTel = false; hasRsrch = false; hasWork = false; hasTeach = false;
				    		hasUnderDeg = false; hasMasDeg = false; hasDoc = false;
				    	}
				    }	
				    
			   }
			   //for(String s: arrayList) { 
				//	System.out.println(s);
			   //}
		   }
		 	catch (Exception e) {
			            System.out.println("Failed: " + e);
		   }
	}
}

	