import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class KFrequentWords { 
	public static void main(String[] args) throws IOException{ 

		long start = Instant.now().toEpochMilli();

		Files.lines(Paths.get("src/data_32GB.txt"))
		//split by space stream 
		.flatMap(currentline -> Stream.of(currentline.split("\\s+"))).parallel()
		//concurrent hash map
		.collect(Collectors.toConcurrentMap(chunk -> chunk, chunk -> 1, Integer::sum))
		.entrySet().stream()
		//sort the values 
		.sorted((s1, s2) -> s1.getValue() == s2.getValue() ? s1.getKey().compareTo(s2.getKey()) : s2.getValue() - s1.getValue())
		//limit to 100 words
		.limit(100).forEach(System.out::println);
//execution time calculation
		long end = Instant.now().toEpochMilli();

		System.out.println(String.format("Executed in %d seconds", (end-start)/1000));
	} 
}