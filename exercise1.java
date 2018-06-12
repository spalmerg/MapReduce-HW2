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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class exercise1 extends Configured implements Tool {
	
	public static class BookValues implements Writable{
		private int occurs; 
		private int volumes;
		
		public BookValues() {
			this.occurs = 0;
			this.volumes = 0;
		}
		public BookValues(int occurs, int volumes) {
			this.occurs = occurs;
			this.volumes = volumes;
		}
		public void set(int occurs, int volumes) {
			this.occurs = occurs;
			this.volumes = volumes;
		}
		public int getOccurs() {
			return occurs;
		}
		public int getVolumes() {
			return volumes;
		}
		public void readFields(DataInput in) throws IOException {
	    		occurs = in.readInt();
	    		volumes = in.readInt();
	    }
	    public void write(DataOutput out) throws IOException {
	    		out.writeInt(volumes);
	    		out.writeInt(occurs);
	    } 
	    public String toString() {
	    		return "occurs: " + occurs + " volumes: " + volumes; 
	    }
	}

    public static class Map1 extends Mapper<Object, Text, Text, BookValues> {

	    	private Text k = new Text();
		private BookValues v = new BookValues();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String[] line = value.toString().split("\\s+");
		    String[] values = {"nu", "chi", "haw"};
		    
		    for(String substring : values){
		    	   if(line[0].toLowerCase().contains(substring)){
		    		   if(line[1].matches("\\d{4}")) {
			    		   String temp_k = substring + "," + line[1];
			    		   int occurs = Integer.parseInt(line[2]);
			    		   int volumes = Integer.parseInt(line[3]);
			    		   
			    		   k.set(temp_k);
			    		   v.set(occurs, volumes);
			    		   context.write(k, v);
		    		   }
		    	   }
		    	}
		}
    }
    
    public static class Map2 extends Mapper<Object, Text, Text, BookValues> {

	    	private Text k = new Text();
		private BookValues v = new BookValues();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String[] line = value.toString().split("\\s+");
		    String[] values = {"nu", "chi", "haw"};
		    
		    for(String substring : values){
		    	   if(line[0].toLowerCase().contains(substring)){
		    		   if(line[2].matches("\\d{4}")) {
			    		   String temp_k = substring + "," + line[2];
			    		   int occurs = Integer.parseInt(line[3]);
			    		   int volumes = Integer.parseInt(line[4]);
			    		   
			    		   k.set(temp_k);
			    		   v.set(occurs, volumes);
			    		   context.write(k, v);
		    		   }
		    	   }
		    	   if(line[1].toLowerCase().contains(substring)){
		    		   if(line[2].matches("\\d{4}")) {
			    		   String temp_k = substring + "," + line[2];
			    		   int occurs = Integer.parseInt(line[3]);
			    		   int volumes = Integer.parseInt(line[4]);
			    		   
			    		   k.set(temp_k);
			    		   v.set(occurs, volumes);
			    		   context.write(k, v);
		    		   }
		    	   }
		    	}
		}
	}

    public static class Combine extends Reducer<Text, BookValues, Text, BookValues> {

		public void reduce(Text key, Iterable<BookValues> values, Context context) throws IOException, InterruptedException {
		    int occurances = 0;
		    int volumes = 0;
		    BookValues result = new BookValues();
		    for(BookValues value: values) {
		    		occurances += value.getOccurs();
		    		volumes += value.getVolumes();
		    }
		    result.set(occurances, volumes);
		    context.write(key, result);
		}

    }
    
    public static class Reduce extends Reducer<Text, BookValues, Text, FloatWritable> {

		public void reduce(Text key, Iterable<BookValues> values, Context context) throws IOException, InterruptedException {
		    int occurances = 0;
		    int volumes = 0;
		    for(BookValues value: values) {
		    		occurances += value.getOccurs();
		    		volumes += value.getVolumes();
	    }
		    float average = (float)occurances/volumes;
		    context.write(key, new FloatWritable(average));
		}
		
    }
    
    public int run(String[] args) throws Exception {
        
        //MULTIPLE INPUTS
		Configuration conf = getConf();
	
		// create job
		Job job = new Job(conf, "exercise1");
		job.setJarByClass(exercise1.class);
		
		// setup map/reduce/combine
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		// specify key/value mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BookValues.class);
		
		// specify key/value reducer
    		job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(FloatWritable.class);
	
	    	// set input
	    	MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
	    	MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);

		// set output
	    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// execute job and return status
	    return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise1(), args);
		System.exit(res);
    }
}