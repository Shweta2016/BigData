import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//To remove warning logs of Spark on console
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		//Create Java Spark Context
		SparkConf sc = new SparkConf().setAppName("Line count").setMaster("local[4]").set("spark.executor.memory","1g");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		/******QUERY1: Number of minor revisions*******/
		//Start time to display execution time of query1
		long startTime = System.currentTimeMillis();
		
		//Read input file as text file for Query1
		JavaRDD<String> lines = jsc.textFile("/Users/asr/Desktop/Test/SparkFiles/Wiki_data_dump_32GB.xml");
		//Filter lines with minor tag
		JavaRDD<String> newTrainData = lines.filter(x -> x.contains("<minor />"));
		//Print count of minor tag
		long minorCnt = newTrainData.count();
		System.out.println("Number of minor revisions: " + (minorCnt-1)); //Minus 1 to remove title in output
		
		//End time to display execution time of query1
	    long stopTime = System.currentTimeMillis();
		
		//Display execution time for query1
	    long elapsedTime = (stopTime - startTime)/1000;	//ms to sec
	    long hours = elapsedTime / 3600;
	    long minutes = (elapsedTime / 60) % 60;
	    long sec = elapsedTime % 60;
	    System.out.println("Execution Time:" + hours + ":" + minutes + ":" + sec);

		//Create spark session
		SparkSession spark = SparkSession.builder().appName("XML Data").master("local[4]").getOrCreate();
		
		/******QUERY2: Number of pages with atmost 5 url links*******/
		//Start time to display execution time of query1
		long startTime2 = System.currentTimeMillis();
		
		//Read xml input files
		Dataset<Row> df = spark.read()
		  .format("xml")
		  .option("rowTag", "page")
		  .load("/Users/asr/Desktop/Test/SparkFiles/Wiki_data_dump_32GB.xml");
		
		//Create a view to use for sql query
		df.createOrReplaceTempView("Pages_Table");
		//Spark sql query to to get pages with atmost 5 urls
		Dataset<Row> ans = spark.sql("select p3.id, p3.title, (LENGTH(p3.revision.text._value) - "
				+ "LENGTH(REPLACE(p3.revision.text._value, 'http', ''))) / LENGTH('http') as httpCount from Pages_Table p3")
				.filter("httpCount < 6 ");	
		
	    
		//Get the row count from sql output
		long urlCount = ans.count();
		//Display number of resulting pages with above query2
		System.out.println("Number of pages with atmost 5 url Links: " + (urlCount - 1));  //Minus 1 to remove title from output
		
		//End time to display execution time of query1
	    long stopTime2 = System.currentTimeMillis();
		
		//Display execution time
	    long elapsedTime2 = (stopTime2 - startTime2)/1000;	//ms to sec
	    long hours2 = elapsedTime2 / 3600;
	    long minutes2 = (elapsedTime2 / 60) % 60;
	    long sec2 = elapsedTime2 % 60;
	    System.out.println("Execution Time:" + hours2 + ":" + minutes2 + ":" + sec2);

		//Save output in output file
		ans.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("outputQ2.csv");
		
		//Close java spark context
		jsc.close();
		
	}

}
