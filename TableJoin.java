import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TableJoin {
	
	public static class StdMarkWritable implements Writable {

		private Text name;
		private IntWritable assign_1;
		private IntWritable assign_2;
		
		public StdMarkWritable() {
			this.name = new Text("");
			this.assign_1 = new IntWritable(-1);
			this.assign_2 = new IntWritable(-1);
		}
		
		public StdMarkWritable(Text name, IntWritable assign_1, IntWritable assign_2) {
			super();
			this.name = name;
			this.assign_1 = assign_1;
			this.assign_2 = assign_2;
		}

		public Text getName() {
			return name;
		}

		public void setName(Text name) {
			this.name = name;
		}

		public IntWritable getAssign_1() {
			return assign_1;
		}

		public void setAssign_1(IntWritable assign_1) {
			this.assign_1 = assign_1;
		}

		public IntWritable getAssign_2() {
			return assign_2;
		}

		public void setAssign_2(IntWritable assign_2) {
			this.assign_2 = assign_2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.name.readFields(data);
			this.assign_1.readFields(data);
			this.assign_2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.name.write(data);
			this.assign_1.write(data);
			this.assign_2.write(data);
		}
		
		public String toString() {
			return this.name.toString() + " " + this.assign_1.toString() + " " + this.assign_2.toString();
		}

		
	}

	public static class StudentMapper extends Mapper<LongWritable, Text, Text, StdMarkWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, StdMarkWritable>.Context context)
				throws IOException, InterruptedException {
			
			String [] parts = value.toString().split(",");
			StdMarkWritable val = new StdMarkWritable();
			val.setName(new Text(parts[1]));
			context.write(new Text(parts[0]),val);
		}		
	}
	
	public static class MarkMapper extends Mapper<LongWritable, Text, Text, StdMarkWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, StdMarkWritable>.Context context)
				throws IOException, InterruptedException {
			
			String [] parts = value.toString().split(",");
			StdMarkWritable val = new StdMarkWritable();
			val.setAssign_1(new IntWritable(Integer.parseInt(parts[1])));
			val.setAssign_2(new IntWritable(Integer.parseInt(parts[2])));
			context.write(new Text(parts[0]), val);
		}		
	}
	
	public static class MyReducer extends Reducer<Text, StdMarkWritable, Text, StdMarkWritable> {

		@Override
		protected void reduce(Text key, Iterable<StdMarkWritable> values,
				Reducer<Text, StdMarkWritable, Text, StdMarkWritable>.Context context) throws IOException, InterruptedException {
			
			StdMarkWritable value = new StdMarkWritable();
			
			for(StdMarkWritable a: values)
				if(a.getName().toString().equals("")) {
					value.setAssign_1(new IntWritable(a.getAssign_1().get()));
					value.setAssign_2(new IntWritable(a.getAssign_2().get()));

				} else {
					value.setName(new Text(a.getName().toString()));
				}
			
			context.write(key, value);
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Table Join");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StdMarkWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StdMarkWritable.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MarkMapper.class);
		
		job.setReducerClass(MyReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
		

	}

}
