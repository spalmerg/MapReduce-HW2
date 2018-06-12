import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class exercise4 extends Configured implements Tool {
	
	
    public static class Map extends Mapper<Object, Text, Text, FloatWritable> {
		private Text artist = new Text();
		private FloatWritable duration = new FloatWritable(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String[] line = value.toString().split(",");
		    
		    artist.set(line[2].toLowerCase());
		    duration.set(Float.parseFloat(line[3]));
		    context.write(artist, duration);
		    
		    }
		}
    
	public static class CustomPartitioner extends Partitioner<Text, FloatWritable> {
	
	    	public int getPartition(Text key, FloatWritable value, int numOfReducers) {
	    		String str = key.toString().replaceAll("\\s+","").toLowerCase();
	    		
	    		if(Character.isDigit(str.charAt(0))) {
	    			return 0;
	    		}
	    		if(str.charAt(0) <= 'f'){
	    			return 1;
	    		}
	    		if(str.charAt(0) <= 'l'){
	    			return 2;
	    		}
	    		if(str.charAt(0) <= 'q'){
	    			return 3;
	    		}
	    		else{
	    			return 4;
	    		}
	    	}
	}
	
    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float max = 0;
		    for(FloatWritable value: values) {
		    		if(value.get() > max) {
		    			max = value.get();
		    		}
		    }
			context.write(key, new FloatWritable(max));
		}
		
    }
    
    public int run(String[] args) throws Exception {
  		// because Tool
		Configuration conf = getConf();
		
	
		// create job
		Job job = new Job(conf, "exercise4");
		job.setJarByClass(exercise4.class);
		
		// setup map
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setPartitionerClass(CustomPartitioner.class);
		
		// specify key/value reducer
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(FloatWritable.class);
	    	job.setNumReduceTasks(5);

	    	// set input
	    	FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    	job.setInputFormatClass(TextInputFormat.class);
	
		// set output
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// execute job and return status
	    return job.waitForCompletion(true) ? 0 : 1;
	}

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise4(), args);
		System.exit(res);
    }
	
}


