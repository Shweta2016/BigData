package nu1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

import scala.Tuple2;


public class MainClass {
	public static final String host = "127.0.0.1";
	public static final int port = 9876;
	public static long lineCount;
	static int a = 0;
	static String tm1;
	
	static int windowSize =60;
    static int slideSize=60;
	static String windowDuration = windowSize + " seconds";
    static String slideDuration = slideSize + " seconds";

	public static void main(String[] args) throws InterruptedException {
		//To remove warning logs of Spark on console
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		System.out.println("In receive main start");
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RTAnalysis");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(60000));
        JavaReceiverInputDStream<String> lines = jssc.receiverStream(new ReceiverClass(port));
      //Split packets into line
        JavaDStream<String> line = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			ArrayList<String> newList = new ArrayList<String>();
			public Iterator<String> call(String x) {
                return Lists.newArrayList(x.split("\n")).iterator();
            }
        });
      //Split line into words and get time and octate
        JavaPairDStream<String,String> timeDoctets=line.mapToPair(
		new PairFunction<String,String,String>(){
			
			@Override
			public Tuple2<String,String> call(String arg0)
					throws Exception {
				String startTime = "";
				String doctet = "";
				
				String[] tuple=arg0.split(",");
				if(tuple.length == 29){
				startTime=(tuple[3]);
				doctet=(tuple[15]);
    			}
						return new Tuple2<String,String>(startTime, doctet);
		}
		});
        
        
        
        lines.print();
        timeDoctets.print();
        
        timeDoctets.foreachRDD((rdd, time) -> {
        	  // Get the singleton instance of SparkSession
        	SparkSession spark = SparkSession
  				  .builder().master("local[4]")
  				  .appName("JavaStreaming")
  				  .getOrCreate();

        	  // Convert RDD[String] to RDD[case class] to DataFrame
        	  JavaRDD<JavaRow> rowRDD = rdd.map(word -> {
        	    JavaRow record = new JavaRow();
        	    record.setTime(word);
        	    record.setOctate(word);
        	    return record;
        	  });
        	   
        	  Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);
        	  // Creates a temporary view using the DataFrame
        	  wordsDataFrame.createOrReplaceTempView("words");
        	  
        	  Dataset<Row> sqlDF1=spark.sql("select  to_timestamp(words.time/1000) as time1,octate from words");
        	  sqlDF1.persist();
        	  Dataset<Row> lines2 =sqlDF1.withWatermark("time1", "10 minutes").groupBy(functions.window(sqlDF1.col("time1"),"60 seconds","60 seconds")
        			  ).agg(functions.avg(sqlDF1.col("octate")).alias("MOVINGAVERAGE"));

        	  lines2.createOrReplaceTempView("rev3");
        	  Dataset<Row> sqlDF3 = spark.sql("SELECT window.start,window.end,MOVINGAVERAGE FROM rev3");
        	  sqlDF3.show();
        	  
        	  Dataset<Row> lines3 =sqlDF1.withWatermark("time1", "10 minutes").groupBy(functions.window(sqlDF1.col("time1"),"60 seconds","60 seconds")
        			  ).agg(functions.max(sqlDF1.col("octate")).alias("MOVINGMAX"));

        	  lines3.createOrReplaceTempView("rev4");
        	  Dataset<Row> sqlDF4 = spark.sql("SELECT window.start,window.end,MOVINGMAX FROM rev4");
        	  sqlDF4.show();
      			
        	});
    
        
        jssc.start();
        System.out.println("In receive main before avait");
        jssc.awaitTermination();
	System.out.println(lineCount);
	}

}


