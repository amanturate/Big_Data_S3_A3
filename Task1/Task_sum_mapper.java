package assignment_3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class Task_sum_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text Company;
	IntWritable units;
	
	@Override
	public void setup(Context context){
		Company = new Text();
		units = new IntWritable();
	}
	
	@Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] lineArray = value.toString().split("\\|");
		
		if((lineArray.length>0)
				&& (lineArray[0] !=null)
				&& (lineArray[1] !=null)
				&& (!lineArray[0].equalsIgnoreCase("NA"))
				&& (!lineArray[1].equalsIgnoreCase("NA"))
				){
			
			Company.set(lineArray[0]);
			units.set(Integer.parseInt(lineArray[2]));
			
			context.write(new Text(Company), units);
			
		}
	}
	
 
}
