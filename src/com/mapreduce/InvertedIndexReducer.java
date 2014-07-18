package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String locations = "";
        for (Text val : values) {
            if(!locations.contains(val.toString())) {
            	locations +=val.toString()+",";
            }
        }
        context.write(key, new Text(locations));
    }
}
