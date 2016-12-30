package offencepack;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class OffencePercentage {
	
		public static class MapClass extends Mapper <LongWritable,Text,Text,LongWritable>{
			
			public void map(LongWritable Key,Text value,Context context)
			{
				try
				{
					String line=value.toString();
					String[] lineParts=line.split(",");
					long speed=Long.parseLong(lineParts[1]);
					context.write(new Text(lineParts[0]), new LongWritable(speed));
				}
				catch(Exception e)
				{
					System.out.println(e.getMessage());
				}
			}
		}
			
			//Finding the percentage of speed violation
			
			public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
			{
				static long count=0;
				static long total=0;
				public void reduce(Text Key,Iterable<LongWritable>values,Context context)throws IOException, InterruptedException 
				{ 
					for(LongWritable val:values)
					{
						if(val.get()>65)
						{
							count++;
						}
						total++;
					}
					long speedingcars=(count*100)/total;
					context.write(Key, new LongWritable(speedingcars));
				}
			}
			
			public static void main(String[] args) throws Exception {
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf, "Speed Check");
				job.setJarByClass(OffencePercentage.class);
				job.setMapperClass(MapClass.class);
				job.setReducerClass(ReduceClass.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(LongWritable.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
}