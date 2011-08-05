package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.*;

public class GlobalClustering {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	if(line.trim().isEmpty())
	    		return;
	    	
	    	StringTokenizer tokenizer = new StringTokenizer(line);
    		String startNode = tokenizer.nextToken().trim();	
    		String endNode = tokenizer.nextToken().trim();
    		
    		if(startNode.equals("#"))
    			return;
    		
    		output.collect(new LongWritable(Long.parseLong(startNode)), new Text(endNode));
    		output.collect(new LongWritable(Long.parseLong(endNode)), new Text(startNode));			
    	}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			HashSet<Integer> nodeSet = new HashSet<Integer>();
			
	        while (values.hasNext()) {
	        	Integer node = new Integer(values.next().toString());
	        	nodeSet.add(node);
	        }
	        
	        Object[] nodeArray = nodeSet.toArray();
	        
	        int size = nodeArray.length;
	        for(int i = 0; i < size - 1; i++){
	        	Integer node1 = (Integer) nodeArray[i];
	        	
	        	for(int j = i+1; j < size; j++){
	        		Integer node2 = (Integer) nodeArray[j];
	        		
	        		output.collect(new LongWritable(node1.longValue()), new Text("open " + node2));
	        	}
	        	
	        	output.collect(key, new Text(node1.toString()));
	        }
	        
	        // output last node
	        int last = nodeArray.length - 1;
	        Integer node1 = (Integer) nodeArray[last];
	        output.collect(key, new Text(node1.toString()));
        }
	}
	
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
	    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	StringTokenizer tokenizer = new StringTokenizer(line);
    		String keyString = tokenizer.nextToken().trim();	
    		String val1 = tokenizer.nextToken().trim();
    		
    		if(val1.equals("open")){
    			output.collect(new LongWritable(Long.parseLong(keyString)), new Text(val1 + " " + tokenizer.nextToken()));	
    		}
    		else{
    			output.collect(new LongWritable(Long.parseLong(keyString)), new Text(val1));	
    		}	
    	}
	}
	
	public static class Reduce2 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			HashSet<Integer> nodeSet = new HashSet<Integer>();
			List<Integer> openList = new ArrayList<Integer>();
			
	        while (values.hasNext()) {
	        	StringTokenizer tokenizer = new StringTokenizer(values.next().toString());
	        	String valType = tokenizer.nextToken();
	        	
	        	// determine if triangle is closed
	        	if(valType.equals("open")){
	        		int node = Integer.parseInt(tokenizer.nextToken());
	        		openList.add(node);
	        	}
	        	else{
	        		int node = Integer.parseInt(valType);
	        		nodeSet.add(node);
	        	}
	        }
	        
	        for(int i = 0; i < openList.size(); i++){
	        	int node = openList.get(i);
	        	
	        	if(nodeSet.contains(node)){
        			output.collect(new LongWritable(1), new Text("1"));
        		}
        		else{
        			output.collect(new LongWritable(2), new Text("1"));
        		}
	        }
        }
	}
	
	public static class Map3 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
	    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	StringTokenizer tokenizer = new StringTokenizer(line);
    		String keyString = tokenizer.nextToken().trim();	
    		String valString = tokenizer.nextToken().trim();
    		
    		output.collect(new LongWritable(Long.parseLong(keyString)), new Text(valString));
    	}
	}
	
	public static class Reduce3 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			int count = 0;
			
	        while (values.hasNext()) {
	        	values.next();
	        	count++;
	        }
	        
	        output.collect(key, new Text(String.valueOf(count)));
        }
	}
	
	
	public static void main(String[] args) throws Exception {
		// Job 1
		JobConf conf = new JobConf(GlobalClustering.class);
	    conf.setJobName("GCC1");
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path("inter1"));
	    JobClient.runJob(conf);
	    
		// Job 2
	    JobConf conf2 = new JobConf(GlobalClustering.class);
	    conf2.setJobName("GCC2");
    	conf2.setOutputKeyClass(LongWritable.class);
	    conf2.setOutputValueClass(Text.class);
	    conf2.setMapperClass(Map2.class);
//	    conf2.setCombinerClass(Reduce2.class);
	    conf2.setReducerClass(Reduce2.class);
	    conf2.setInputFormat(TextInputFormat.class);
	    conf2.setOutputFormat(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(conf2, new Path("inter1"));
	    FileOutputFormat.setOutputPath(conf2, new Path("inter2"));
	    JobClient.runJob(conf2);
	    
	    // Job 3
	    JobConf conf3 = new JobConf(GlobalClustering.class);
	    conf3.setJobName("GCC3");
		conf3.setOutputKeyClass(LongWritable.class);
		conf3.setOutputValueClass(Text.class);
		conf3.setMapperClass(Map3.class);
//		conf3.setCombinerClass(Reduce3.class);
		conf3.setReducerClass(Reduce3.class);
		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf3, new Path("inter2"));
	    FileOutputFormat.setOutputPath(conf3, new Path(args[1]));
	    JobClient.runJob(conf3);
	}
}
