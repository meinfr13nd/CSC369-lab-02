import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.io.*;

public class UndistributedSolution {
  public static void main(String[] args) throws IOException  {
    LocalDateTime start = LocalDateTime.now();

    HashMap<String,Integer> hashMap = new HashMap<>();

    File file = new File(args[0]);
    FileReader fr = new FileReader(file);
    BufferedReader br = new BufferedReader(fr);
    String line;
    while( (line = br.readLine()) != null) {
      String[] parts = line.split(", ");
      hashMap.put(parts[1], hashMap.getOrDefault(parts[1], 0) + 1);
    }

    File fout = new File("salesOut.txt");
    FileOutputStream fos = new FileOutputStream(fout);

    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

    List<String> sortedDates = new ArrayList<>(hashMap.keySet());
    Collections.sort(sortedDates);

    for (String date: sortedDates) {
      bw.write(String.format("%s %d%n", date, hashMap.get(date)));
    }

    bw.close();

    LocalDateTime end = LocalDateTime.now();
    long diff = start.until(end, ChronoUnit.MILLIS);
    System.out.println("These sales took " + diff + " milliseconds to process in a non-distributed way.");
  }
}