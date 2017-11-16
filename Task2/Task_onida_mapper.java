package assignment_3_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task_onida_mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text State;
	IntWritable units;
	
	@Override
	public void setup(Context context){
		State = new Text();
		units = new IntWritable();
	}
	
	@Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] lineArray = value.toString().split("\\|");
		
		if((lineArray.length>0)
				&& (lineArray[0].matches("Onida"))
				){
			
			State.set(lineArray[0]+"|"+lineArray[3]);
			units.set(Integer.parseInt(lineArray[2]));
			
			context.write(new Text(State), units);
			
		}
	}
}
