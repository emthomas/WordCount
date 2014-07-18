package com.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text>{
	Text word = new Text();
	Text location = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
		String filename = split.getPath().getName().toString();
		location.set(filename);
        
		String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, location);
        }
	}
}