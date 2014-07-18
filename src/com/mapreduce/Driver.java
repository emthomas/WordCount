package com.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
    public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
        Job job = new Job(conf, "WordCount");
        job.setJarByClass(Driver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Person.class);
 //       job.setOutputValueClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
 
//        job.setMapperClass(WordCountMapper.class);
//        job.setCombinerClass(WordCountReducer.class);
//        job.setReducerClass(WordCountReducer.class);
        
//        job.setMapperClass(InvertedIndexMapper.class);
//        job.setReducerClass(InvertedIndexReducer.class);
        
        job.setMapperClass(ComplexMapper.class);
        job.setReducerClass(ComplexReducer.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        
     // Defines additional single text based output 'text' for the job
        MultipleOutputs.addNamedOutput(job, "QA", TextOutputFormat.class, Text.class, Text.class);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
 
        String input = "input";
        String output = "output";
        
        delete(new File(output));
        
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        // Submit the job, then poll for progress until the job is complete
        boolean success = job.waitForCompletion(true);
        writeQA(job, conf, output);
        return success ? 1 : 0 ;  
    }   
    
    public static void main(String[] args) throws Exception {
    	Driver dr = new Driver();
    	int ret = dr.run(args);
        System.exit(ret);
    }
   
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public void writeQA(Job job, Configuration conf, String output) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(output+"/"+job.getJobName()+".log"));
        
        Counters counters = job.getCounters();
        for (CounterGroup group : counters) {
        	out.writeChars("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")\n");
        	out.writeChars("  number of counters in this group: " + group.size()+"\n");
        	  for (Counter counter : group) {
        		  out.writeChars("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue()+"\n");
        	  }
        }
        
        out.close();
	}
	
	   public static void delete(File file) throws IOException{
		     
       	if(file.isDirectory()){
    
       		//directory is empty, then delete it
       		if(file.list().length==0){
    
       		   file.delete();
       		   System.out.println("Directory is deleted : " 
                                                    + file.getAbsolutePath());
    
       		}else{
    
       		   //list all the directory contents
           	   String files[] = file.list();
    
           	   for (String temp : files) {
           	      //construct the file structure
           	      File fileDelete = new File(file, temp);
    
           	      //recursive delete
           	     delete(fileDelete);
           	   }
    
           	   //check the directory again, if empty then delete it
           	   if(file.list().length==0){
              	     file.delete();
           	     System.out.println("Directory is deleted : " 
                                                     + file.getAbsolutePath());
           	   }
       		}
    
       	}else{
       		//if file, then delete it
       		file.delete();
       		System.out.println("File is deleted : " + file.getAbsolutePath());
       	}
       }

}
