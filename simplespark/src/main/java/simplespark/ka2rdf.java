package simplespark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ka2rdf {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ka2rdf").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> inputFile = sparkContext.textFile("D:/eclipse/sparkWorkspace/simplespark/test.txt");
	}
}
