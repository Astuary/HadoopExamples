import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;

public class HotOrCold{
	
	//Mapper
	/*
	MaxTemperatureMapper class is static and extends Mapper abstract class having
	four Hadoop generics type LongWriteable, Text, Text, Text.
	*/
	public static class MaxMinTemperatureMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		/*
		This method takes the input as text data type.
		Now leaving the first five tokens, it takes 6th token as temp_max and
		7th token as temp_min.
		Now temp_max > 35 and temp_min < 10 are passed to the reducer.
		*/
		public static final int MISSING = 9999;
		
		@Override
		public void map(LongWritable arg0, Text value, Context context) throws IOException, InterruptedException{
			
			// Converting the record (single line) to String and string it in a String variable line
			String line = value.toString();
			
			if(!(line.length() == 0)){
				
				//fetching data
				String date = line.substring(6, 14);
				
				//fetching maximum temperature
				float temp_max = Float.parseFloat(line.substring(39, 45).trim());
				
				//fetching minimum temperature
				float temp_min = Float.parseFloat(line.substring(47, 53).trim());
				
				// if maximum temperature is greater than 35, it's a hot day
				if(temp_max > 35.0 && temp_max != MISSING){
					// Hot Day
					context.write(new Text("Hot Day " + date), new Text(String.valueOf(temp_max)));
				}
				
				// if minimum temperature is less than 10, it's a cold day
				if(temp_min < 10.0 && temp_min != MISSING){
					// Cold Day
					context.write(new Text("Cold Day " + date), new Text(String.valueOf(temp_min)));
				}
				
			}
			
		}
		
	}
	
	public static class MaxMinTemperatureReducer extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Text value, Context context)
			throws IOException,InterruptedException {
			 
			context.write(key, value);
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Austin Weather");
		
		job.setJarByClass(HotOrCold.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(MaxMinTemperatureMapper.class);
		job.setReducerClass(MaxMinTemperatureReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path OutputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		OutputPath.getFileSystem(conf).delete(OutputPath, true);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}