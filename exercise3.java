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


public class exercise3 extends Configured implements Tool {
	
    public static class Map extends Mapper<Object, Text, Text, Text> {
		private Text k = new Text("");
		private Text v = new Text(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String[] line = value.toString().split(",");
		    
		    if(line[165].matches("\\d{4}") &&
		    			Integer.parseInt(line[165]) <= 2010 && 
		    			Integer.parseInt(line[165]) >= 2000) {
			    String result = line[0] + ", " + line[2] + ", " + line[3];
			    
			    context.write(k, new Text(result));
		    }
		}
    }
    
	public int run(String[] args) throws Exception {
  		// because Tool
		Configuration conf = getConf();
		
	
		// create job
		Job job = new Job(conf, "exercise3");
		job.setJarByClass(exercise3.class);
		
		// setup map
	    job.setMapperClass(Map.class);
		
		// specify key/value reducer
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	job.setNumReduceTasks(0);

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
		int res = ToolRunner.run(new Configuration(), new exercise3(), args);
		System.exit(res);
    }
	
}
