import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class KFrequentSubList {
	public static void main(String[] args) 
	{   
		long start, end;
		start = System.currentTimeMillis();

		HashMap<String, Integer> map = new HashMap<String, Integer>();
		//create buffer reader
		BufferedReader bufferReader=null;
		try
		{
			bufferReader = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream("src/data_32GB.txt"))));

			String line = bufferReader.readLine();
			while (line != null)
			{   
				// separate by space
				String[] words = line.split("\\s+");
				//loop through all words
				for (String word : words)	
					map.put(word, map.getOrDefault(word,0)+1);
				line = bufferReader.readLine(); //nextline
			}
			//create a list from map entries
			List<Entry<String, Integer>> maplist = new ArrayList<Entry<String,Integer>>(map.entrySet());
			//custom compare decreasing order of values 
			Collections.sort(maplist, new Comparator<Entry<String, Integer>>() 
			{
				@Override
				public int compare(Entry<String, Integer> a, Entry<String, Integer> b) 
				{
					return (b.getValue().compareTo(a.getValue()));
				}
			});
			//select the top 100 sublists 
			List<Entry<String, Integer>> sublist=maplist.subList(0, 100);
			for (Entry<String, Integer> entry : sublist) 
			{
				System.out.println(entry.getKey() + " : "+ entry.getValue());
			}
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				bufferReader.close();          
			}
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		end = System.currentTimeMillis();
		System.out.println(String.format("Executed in %d seconds", (end-start)/1000));
	}   
}
