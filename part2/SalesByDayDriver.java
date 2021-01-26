import org.apache.log4j.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class SalesByDayDriver extends Configured implements Tool {

    private static final Logger THE_LOGGER = Logger.getLogger(SalesByDayDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();// creates new Hadoop job
        job.setJarByClass(SalesByDayDriver.class);
        job.setJobName("SalesByDayDriver"); //same as java class name
        job.setOutputKeyClass(Text.class); //output key class for reduce function
        job.setOutputValueClass(IntWritable.class); //output value class for reduce function
        job.setMapOutputKeyClass(Text.class); //output key class for map function
        job.setMapOutputValueClass(IntWritable.class); //output value class for map function
        job.setMapperClass(SalesMapper.class);//sets the mapper
        job.setReducerClass(SalesReducer.class);//sets the reducer
        job.setCombinerClass(SalesReducer.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true); //runs the job, returns true if executed successfully 
        THE_LOGGER.info("run(): status=" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: <input> <output>");
        }
        LocalDateTime start = LocalDateTime.now();

        THE_LOGGER.info("inputDir = " + args[0]);
        THE_LOGGER.info("outputDir = " + args[1]);
        int returnStatus = ToolRunner.run(new SalesByDayDriver(), args);

        THE_LOGGER.info("returnStatus=" + returnStatus);

        LocalDateTime end = LocalDateTime.now();
        long diff = start.until(end, ChronoUnit.MILLIS);
        THE_LOGGER.info("These sales took " + diff + " milliseconds to process in a distributed  (with a combiner).");
        System.exit(returnStatus);
    }
}

