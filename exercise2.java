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


public class exercise2 extends Configured implements Tool {
	
	public static class Pair implements Writable{
		private double count; 
		private double sum;
		private double ss; 
		
		public Pair() {
			this.count = 0;
			this.sum = 0;
			this.ss = 0; 
		}
		public Pair(double count, double sum, double ss) {
			this.count = count;
			this.sum = sum;
			this.ss = ss; 
		}
		public void set(double count, double sum, double ss) {
			this.count = count;
			this.sum = sum;
			this.ss = ss; 
		}
		public double getCount() {
			return count;
		}
		public double getSum() {
			return sum;
		}
		public double getSS() {
			return ss;
		}
		public void readFields(DataInput in) throws IOException {
	    		count = in.readDouble();
	    		sum = in.readDouble();
	    		ss = in.readDouble();
	    }
	    public void write(DataOutput out) throws IOException {
	    		out.writeDouble(count);
	    		out.writeDouble(sum);
	    		out.writeDouble(ss);
	    } 
	}

    public static class Map1 extends Mapper<Object, Text, Text, Pair> {
    		private Text k = new Text("");
    		private Pair v = new Pair(); 
    		
    		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		    String[] line = value.toString().split("\\s+");
    		    
    		    double sum = Double.parseDouble(line[3]);
    		    double count = 1; 
    		    double ss = sum * sum; 
    		    
    		    v.set(count, sum, ss);
    		    context.write(k, v);
    		}
    }
    
    public static class Map2 extends Mapper<Object, Text, Text, Pair> {
		private Text k = new Text("");
		private Pair v = new Pair(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String[] line = value.toString().split("\\s+");
		    
		    double sum = Double.parseDouble(line[4]);
		    double count = 1; 
		    double ss = sum * sum; 
		    
		    v.set(count, sum, ss);
		    context.write(k, v);
		}
    }

    public static class Combine extends Reducer<Text, Pair, Text, Pair> {

		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			double count = 0;
			double sum = 0;
			double ss = 0;
		    Pair result = new Pair();
		    
		    for(Pair value: values) {
		    		count += value.getCount();
		    		sum += value.getSum();
		    		ss += value.getSS();
		    }
		    result.set(count, sum, ss);
		    context.write(key, result);
		}

    }
    
    public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			double n = 0;
			double sum = 0;
			double ss = 0;
		    for(Pair value: values) {
		    		n += value.getCount();
		    		sum += value.getSum();
		    		ss += value.getSS();
		    }
		    double temp = sum/n; 
		    temp = ss - (n*temp*temp);
		    temp = (1/n) * temp;
		    temp = Math.sqrt(temp);
			context.write(key, new DoubleWritable(temp));

		}
		
    }
    
    public int run(String[] args) throws Exception {
        
        //MULTIPLE INPUTS
		Configuration conf = getConf();
	
		// create job
		Job job = new Job(conf, "exercise2");
		job.setJarByClass(exercise2.class);
		
		// setup map/reduce/combine
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		// specify key/value mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);
		
		// specify key/value reducer
    		job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(DoubleWritable.class);
	
	    	// set input
	    	MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
	    	MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);

		// set output
	    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// execute job and return status
	    return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise2(), args);
		System.exit(res);
    }
	
}
