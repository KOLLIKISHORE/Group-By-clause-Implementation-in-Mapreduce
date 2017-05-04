package count;
/*
import java.io.*;
import java.util.Random;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.io.IOException;
import java.util.StringTokenizer;
*/
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Table3{

	public static class MyMapper
       extends Mapper<Object, Text, Text, IntWritable>{

   
    public void map(Object offset, Text line, Context context
                    ) throws IOException, InterruptedException {

      String words[]=line.toString().split("<SEP>");
      if(words.length==5){
      
      context.write(new Text(words[3]),new IntWritable(1)); }
}
}

  public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    
    private IntWritable output = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> array,
                        Context context
                       ) throws IOException, InterruptedException {


     int count = 0;

     for(IntWritable i: array)
     {
    	 count += i.get();
     }
     output.set(count);
  
    context.write(new Text(key+"<SEP>"),output);
  }
}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "No of tracks by Year");
    job.setJarByClass(Table3.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}