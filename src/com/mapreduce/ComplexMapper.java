package com.mapreduce;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.enumeration.QACOUNTERS;


public class ComplexMapper extends Mapper<LongWritable, Text, Text, Person>{
	Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
        	String token = tokenizer.nextToken();
            word.set(token);
            
    		Date dob = new Date((int)((Math.random()*50)+60), (int)(Math.random()*11+1), (int)(Math.random()*28+1));
    		TreeMap<String,Integer> tree = new TreeMap<String,Integer>();
    		double limit = Math.random()*10;
    		for(int i=0; i<limit; i++) {
    			tree.put(""+i, (int)(i+Math.random()*1000));
   		}
            
            
            Person person = new Person(Math.random()*50, Math.random()*50, Math.random()*50, dob, tree);
            context.write(word, person);
            if(Math.random()*10<5) {
            	context.getCounter(QACOUNTERS.ERROR).increment(1);
            } else {
            	context.getCounter(QACOUNTERS.SUCCESS).increment(1);
            }
        }
		
	}
	
	public void run(Context context) throws IOException, InterruptedException {
		System.out.println("in run method of mapper");
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}
}
