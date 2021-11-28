package spark.wordcount;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class App {

    private static final String SPARK_HOME = "C:/Tool/spark-3.2.0-bin-hadoop3.26";

    private static final String MASTER_HOST = "local[*]";

    private static final String APP_NAME = "test";

    private final static String SRC_FILE_PATH = "./resources/RomeoAndJuliet.txt";

    private final static String RESULT_FILE_PATH = "./resources/result";

    private static List<String> StopWords = List.of("");

    private final static String REGEX = "[\\p{Punct}\\s]+";

    public static void main(String[] args) {
	initStopWords();
	StopWords = StopWords.stream().map(String::toLowerCase).collect(Collectors.toList());

//	runRDDExample();
	runDFExample();
    }

    private static void initStopWords() {
	// add stop words here
    }

    private static void runDFExample() {

	SparkConf conf = new SparkConf();
	conf.setAppName(APP_NAME);
	conf.setMaster(MASTER_HOST);
	conf.setSparkHome(SPARK_HOME);

	SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

	Dataset<String> lines = spark.read().text(SRC_FILE_PATH).as(Encoders.STRING());

	Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	    @Override
	    public Iterator<String> call(String t) throws Exception {
		return Arrays.asList(t.split(REGEX)).iterator();
	    }

	}, Encoders.STRING()).filter(new FilterFunction<String>() {

	    @Override
	    public boolean call(String value) throws Exception {
		return !StopWords.contains(value);
	    }

	});

	Dataset<Row> rows = words.groupBy("value").count().toDF("word", "count");

	rows.toJavaRDD().saveAsTextFile(RESULT_FILE_PATH);

    }

    private static void runRDDExample() {
	SparkConf conf = new SparkConf();
	conf.setAppName(APP_NAME);
	conf.setMaster(MASTER_HOST);
	conf.setSparkHome(SPARK_HOME);
	var sc = new JavaSparkContext(conf);

	JavaRDD<String> lines = sc.textFile(SRC_FILE_PATH, 0);

	lines.cache();

	JavaPairRDD<String, Integer> pairs = lines.flatMap(new FlatMapFunction<String, String>() {
	    @Override
	    public Iterator<String> call(String t) throws Exception {
		return Arrays.asList(t.split(REGEX)).iterator();
	    }

	}).filter(word -> !StopWords.contains(word)).mapToPair(new PairFunction<String, String, Integer>() {

	    @Override
	    public Tuple2<String, Integer> call(String t) throws Exception {
		return new Tuple2<>(t.toLowerCase(), 1);
	    }
	}).reduceByKey(new Function2<Integer, Integer, Integer>() {

	    @Override
	    public Integer call(Integer v1, Integer v2) throws Exception {
		return v1 + 1;
	    }
	});

	pairs.saveAsTextFile(RESULT_FILE_PATH);
    }
}
