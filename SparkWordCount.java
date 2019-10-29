import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

//	public static class FirstMapper implements FlatMapFunction<String, String> {
//
//		@Override
//		public Iterator<String> call(String sentence) throws Exception {
//			return Arrays.asList(sentence.split(" ")).iterator();
//		}
//	}
//	
//	public static class SecondMapper implements PairFunction<String, String, Integer> {
//
//		@Override
//		public Tuple2<String, Integer> call(String token) throws Exception {
//			return new Tuple2<String, Integer>(token, 1);
//		}
//	}
//	
//	public static class ReducerFunction implements Function2<Integer, Integer, Integer> {
//		@Override
//		public Integer call(Integer a, Integer b) throws Exception {
//			return a + b;
//		}
//	}
//	
//	public static class SwapMapper implements PairFunction<Tuple2<String, Integer>, Integer, String> {
//
//		@Override
//		public Tuple2<Integer, String> call(Tuple2<String, Integer> pair) throws Exception {
//			return pair.swap();
//		}
//		
//	}
	
	public static class IntegerCompartor implements Comparator<Integer>, Serializable {

		@Override
		public int compare(Integer o1, Integer o2) {
			return o1- o2;
		}
		
	}
	
	public static void main(String [] args) {
		
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("Word Count");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		context.textFile(args[0])
				.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
				.mapToPair(t -> new Tuple2<>(t, 1))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(a -> a.swap())
				.sortByKey(new IntegerCompartor())
				.saveAsTextFile(args[1]);
		
	}
}
