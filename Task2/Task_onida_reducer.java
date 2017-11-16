package assignment_3_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task_onida_reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

Integer sum;
	
	public void setup (Context context){
		sum = new Integer(0);
	}
	//To get key wise sum of units
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		Integer sum = 0;
		for(IntWritable value : values){
			sum = sum + value.get();
		}
		
		context.write(key, new IntWritable(sum));
	}

}
