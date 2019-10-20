import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountSort {
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String [] tokens =  value.toString().split(" ");
			
			for(String token: tokens) {
				context.write(new Text(token), new IntWritable(1));
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text token, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			int sum = 0;
					
			for(IntWritable val: values)
				sum += val.get();
			
			context.write(token, new IntWritable(sum));
		}
		
	}
	
	
	public static class MyComparator extends WritableComparator {

		public MyComparator() {
			super(Text.class, true);
		}
	
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text t1 = (Text) a;
			Text t2 = (Text) b;
			
			return  -1 * t1.compareTo(t2);
		}	
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyReducer.class);
		job.setMapperClass(MyMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setSortComparatorClass(MyComparator.class);
		
		job.waitForCompletion(true);
	}

}

