package com.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class Person implements Writable {
	  public DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	  
	  private double x;
	  private double y;
	  private double z;
	  private Date dob;
	  private TreeMap<String,Integer> tree;

	  public Person(double d, double e, double f, Date newDob, TreeMap<String,Integer> tree) {
	    this.x = d;
	    this.y = e;
	    this.z = f;
	    this.dob = newDob;
	    this.tree = tree;
	  }

	  public Person() {
	    this(0.0d, 0.0d, 0.0d, new Date(), new TreeMap<String,Integer>());
	  }

	  public void write(DataOutput out) throws IOException {
	    out.writeDouble(x);
	    out.writeDouble(y);
	    out.writeDouble(z);
	    out.writeUTF(df.format(dob));
	    out.writeInt(tree.size());
	    for(Entry<String,Integer> entry : tree.entrySet()) {
	    	out.writeUTF(entry.getKey());
	    	out.writeInt(entry.getValue());
	    }
	  }

	  public void readFields(DataInput in) throws IOException {
	    x = in.readDouble();
	    y = in.readDouble();
	    z = in.readDouble();
	    try {
			dob = df.parse(in.readUTF());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    tree.clear();
	    
	    int count = in.readInt();
	    
	    while(count-- > 0) {
	    	String key = in.readUTF();
	    	Integer value = in.readInt();
	    	tree.put(key, value);
	    }
	  }

	  public String toString() {
	    return Double.toString(x) + ", "
	        + Double.toString(y) + ", "
	        + Double.toString(z) + ", "
	        + df.format(dob) + ", "
	        + tree;
	  }
	}
