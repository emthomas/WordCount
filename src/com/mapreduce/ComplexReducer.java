package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;

public class ComplexReducer extends Reducer<Text, Person, Text, Person> {
	private MultipleOutputs<Text, Person> mos;
	
	public void setup(Context context) {
		 mos = new MultipleOutputs<Text, Person>(context);
	}

	public void reduce(Text key, Iterable<Person> values, Context context) throws IOException, InterruptedException {
		for(Person person : values) {
			mos.write(key, person, generateFileName(key, person));
			context.getCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").increment(1);
		}
		 
	}
	
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 mos.close();
	}
	
	 public String generateFileName(Text k, Person v) {
		   return "partition="+k.toString()+"/";
	}
}
