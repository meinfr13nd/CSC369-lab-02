import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;


public class SalesReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
   
   @Override
   public void reduce(Text key,
	Iterable<IntWritable> integers, Context context)
        throws IOException, InterruptedException {
     int sum = 0;
     for (IntWritable i:integers) {
       sum += i.get();
     }
     context.write(key, new IntWritable(sum));
    }
}

