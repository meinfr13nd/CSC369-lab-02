import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class SalesMapper extends
          Mapper<LongWritable, Text, Text, IntWritable> {
   private static Logger THE_LOGGER = Logger.getLogger(DivisibleByThreeDriver.class);

   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
      THE_LOGGER.debug("I AM IN LOGGER");
      String valueAsString = value.toString().trim();
      String[] tokens = valueAsString.split(", ");
      context.write(new Text(tokens[1]), new IntWritable(1));
   }
}


