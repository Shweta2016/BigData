package com.wordCount;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;

/* Class to print top 100 most frequent words in given input file*/
public class MostFreqWords {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//Start time to display execution time of program
		long startTime = System.currentTimeMillis();
		
		FileInputStream inputStream = null;
		Scanner sc = null;
		String[] inputRow = null;
		HashMap<String, Integer> map = new HashMap<>();
		
		try {
		    //Read input file
			//Note: please enter the path of your file below before running the program
			inputStream = new FileInputStream("/Users/asr/Desktop/Test/Files/DataSet/data_1GB.txt");
		    sc = new Scanner(inputStream);
		    //Read each line in file and get each word
		    while (sc.hasNextLine()) {
		        String line = sc.nextLine();
		        inputRow = line.split("\\s+");
		        //Put each word in map with its count
			    for (String wrd:inputRow) {
			    	if(map.containsKey(wrd)){
		                map.put(wrd, map.get(wrd)+1);
		            }else{
		                map.put(wrd, 1);
		            }
		        }
		    }
		    //Min-heap
	        PriorityQueue<WordCount> queue = new PriorityQueue<>(Comparator.comparing(WordCount->WordCount.count));
	 
	        //Heap of size k=100
	        for(Map.Entry<String, Integer> entry: map.entrySet()){
	        	WordCount p = new WordCount(entry.getKey(), entry.getValue());
	            queue.offer(p);
	            if(queue.size()>100){
	                queue.poll();
	            }
	        }
	 
	        //Get all elements from the heap
	        List<String> result = new ArrayList<>();
	        while(queue.size()>0){
	            result.add(queue.poll().word);
	            
	        }
	 
	        //Reverse the order
	        Collections.reverse(result);
	        
	        //End time to display execution time of program
	        long stopTime = System.currentTimeMillis();
	        
	        //Display execution time
	        long elapsedTime = (stopTime - startTime)/1000;	//ms to sec
	        long hours = elapsedTime / 3600;
	        long minutes = (elapsedTime / 60) % 60;
	        long sec = elapsedTime % 60;
	        System.out.println("Execution Time:" + hours + ":" + 
	        minutes + ":" + sec);
	        
	        //Display final top 100 words with maximum frequency
	        System.out.println("The top 100 most frequent words are:");
	        for(String str:result){
	        	System.out.println(str +" "+ map.get(str));
	        }
	        
	        
	        
		} finally {
		    if (inputStream != null) {
		        inputStream.close();
		    }
		    if (sc != null) {
		        sc.close();
		    }
		}

	}

}

/* Class to define word count object which has words and count of each word*/
class WordCount{
	String word;
	int count;
	public WordCount(String word, int count){
		this.word = word;
		this.count = count;
	}
}
