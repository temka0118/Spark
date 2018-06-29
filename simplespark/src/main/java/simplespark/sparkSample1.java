package simplespark;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class sparkSample1 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		/*JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
		  public Integer call(Integer x) { return x*x; }
		});*/
	
		
		Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer x, Integer y) { return x + y; }
		});
		
		JavaDoubleRDD result = rdd.mapToDouble(
				  new DoubleFunction<Integer>() {
				    public double call(Integer x) {
				      return (double) x * x;
				    }
				});
		System.out.println(result.collect());
		//System.out.println(sum);
	}
 
}
