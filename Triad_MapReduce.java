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

public class Triad_MapReduce {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		LongWritable n1 = new LongWritable();
		LongWritable n2 = new LongWritable();
		Text e1 = new Text();
		Text e2 = new Text();
		
	    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	    	
	    	String line = value.toString();
	    	if(line.trim().isEmpty()){
	    		return;
	    	}
	    	
	    	StringTokenizer tokenizer = new StringTokenizer(line);
    		String startNode = tokenizer.nextToken().trim();	
    		String endNode = tokenizer.nextToken().trim();

    		if(startNode.equals("#"))
    			return;
    		
    		n1.set(Integer.parseInt(startNode));
    		n2.set(Integer.parseInt(endNode));
    		e1.set(endNode + " " + "out");
    		e2.set(startNode + " " + "in");
    		
    		output.collect(n1, e1);
    		output.collect(n2, e2);
    	}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			HashMap<Integer, String> nodeMap = new HashMap<Integer, String>();
			int triads = 0;
			
	        while (values.hasNext()) {
	        	StringTokenizer tokenizer = new StringTokenizer(values.next().toString());
	        	Integer node = new Integer(tokenizer.nextToken());
	        	String edge = tokenizer.nextToken();
	        	
	        	if(nodeMap.containsKey(node)){
        			nodeMap.put(node, "sym");
	        	}
	        	else{
	        		nodeMap.put(node, edge);
	        	}
	        }
	        
	        Object[] nodeArray = nodeMap.keySet().toArray();
	        Object[] valArray = nodeMap.values().toArray();
	        for(int i = 0; i < nodeArray.length - 1; i++){
	        	Integer key1 = (Integer) nodeArray[i];
	        	String val1 = (String) valArray[i];
	        	
	        	for(int j = i+1; j < nodeArray.length; j++){
	        		Integer key2 = (Integer) nodeArray[j];
	        		String val2 = (String) valArray[j];
	        		
	        		int type = 11; // sym && sym
	        		
	        		if(val1.equals("out") && val2.equals("out")){
	        			type = 4;
	        			output.collect(new LongWritable(key1) , new Text("open " + key2 + " " + type));
	        		}
	        		else if(val1.equals("in") && val2.equals("in")){
	        			type = 5;
	        			output.collect(new LongWritable(key1) , new Text("open " + key2 + " " + type));
	        		}
	        		else if(val1.equals("in") && val2.equals("out")){
	        			type = 6;
	        			output.collect(new LongWritable(key2) , new Text("open " + key1 + " " + type));
	        		}
	        		else if(val1.equals("out") && val2.equals("in")){
	        			type = 6;
	        			output.collect(new LongWritable(key1) , new Text("open " + key2 + " " + type));
	        		}
	        		else if(val1.equals("sym") && val2.equals("in")){
	        			type = 7;
	        			output.collect(new LongWritable(key1) , new Text("open " + key2 + " " + type));
	        		}
	        		else if(val1.equals("in") && val2.equals("sym")){
	        			type = 7;
	        			output.collect(new LongWritable(key2) , new Text("open " + key1 + " " + type));
	        		}
	        		else if(val1.equals("sym") && val2.equals("out")){
	        			type = 8;
	        			output.collect(new LongWritable(key1) , new Text("open " + key2 + " " + type));
	        		}
	        		else if(val1.equals("out") && val2.equals("sym")){
	        			type = 8;
	        			output.collect(new LongWritable(key2) , new Text("open " + key1 + " " + type));
	        		}
	        		else if(val1.equals("sym") && val2.equals("sym")){
	        			type = 11;
	        			output.collect(new LongWritable(key1) , new Text("open " + key2 + " " + type));
	        		}

	        		triads++;
	        	}
	        	
	        	output.collect(key, new Text(key1 + " " + val1));
	        }
	        
	        int last = nodeArray.length - 1;
	        Integer key1 = (Integer) nodeArray[last];
        	String val1 = (String) valArray[last];
	        output.collect(key, new Text(key1 + " " + val1));
        }
	}
	
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		LongWritable keyOutput = new LongWritable();
		Text valOutput = new Text();
		
	    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	StringTokenizer tokenizer = new StringTokenizer(line);
    		String keyString = tokenizer.nextToken().trim();	
    		String val1 = tokenizer.nextToken().trim();
    		
    		if(val1.equals("open")){
    			valOutput.set(val1 + " " + tokenizer.nextToken().trim() + " " + tokenizer.nextToken().trim());
    		}
    		else{
    			valOutput.set(val1 + " " + tokenizer.nextToken().trim());
    		}
    		
    		keyOutput.set(Long.parseLong(keyString));
    		output.collect(keyOutput, valOutput);		
    	}
	}

	public static class Reduce2 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			HashMap<Integer, String> nodeMap = new HashMap<Integer, String>();
			List<Integer> nodeList = new ArrayList<Integer>();
			List<String> typeList = new ArrayList<String>();
			
	        while (values.hasNext()) {
	        	Text val = values.next();
	        	StringTokenizer tokenizer = new StringTokenizer(val.toString());
	        	String valType = tokenizer.nextToken();
	        	
	        	if(valType.equals("open")){
	        		int node = Integer.parseInt(tokenizer.nextToken());
	        		String type = tokenizer.nextToken();
	        		
	        		nodeList.add(node);
	        		typeList.add(type);
	        	}
	        	else{
	        		Integer k = new Integer(valType);
	        		String v = tokenizer.nextToken();
	        		nodeMap.put(k, v);
	        	}
	        	
	        }
	        
	        for(int i = 0; i < nodeList.size(); i++){
	        	int node = nodeList.get(i);
	        	String type = typeList.get(i);
	        	
	        	if(!nodeMap.containsKey(node)){
        			output.collect(new LongWritable(Integer.parseInt(type)), new Text("1"));
        		}
        		else{
        			String edge = nodeMap.get(node);
        			if(type.equals("4") && edge.equals("sym")){
        				type = "12";
        			}
        			else if(type.equals("4") && !edge.equals("sym")){
        				type = "9";
        			}
        			else if(type.equals("5") && !edge.equals("sym")){
        				type = "9";
        			}
        			else if(type.equals("5") && edge.equals("sym")){
        				type = "13";
        			}
        			else if(type.equals("6") && !edge.equals("sym")){
        				if(edge.equals("in")){
        					type = "9";
        				}
        				else{
        					type = "10";
        				}
        			}
        			else if(type.equals("6") && edge.equals("sym")){
        				type = "14";
        			}
        			else if(type.equals("7") && edge.equals("sym")){
        				type = "15";
        			}
        			else if(type.equals("7") && !edge.equals("sym")){
        				if(edge.equals("in")){
        					type = "12";
        				}
        				else{
        					type = "14";
        				}
        			}
        			else if(type.equals("8") && edge.equals("sym")){
        				type = "15";
        			}
        			else if(type.equals("8") && !edge.equals("sym")){
        				if(edge.equals("in"))
        					type = "14";
        				else
    						type = "13";
        			}
        			else if(type.equals("11") && edge.equals("sym")){
        				type = "16";
        			}
        			else if(type.equals("11") && !edge.equals("sym")){
        				type = "15";
        			}
        			
        			output.collect(new LongWritable(Long.parseLong(type)), new Text("1"));
        		}
	        }
        }
	}
	
	public static class Map3 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
	    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	StringTokenizer tokenizer = new StringTokenizer(line);
    		String keyString = tokenizer.nextToken();	
    		tokenizer.nextToken();
    		
    		output.collect(new LongWritable(Long.parseLong(keyString)), new Text("1"));
    	}
	}
	
	public static class Reduce3 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			int triads = 0;
			
	        while (values.hasNext()) {
	        	values.next();
	        	triads++;
	        }
	        
	        Integer k = new Integer(key.toString());
	        if(k.intValue() == 9 || k.intValue()  == 10 || k.intValue() > 11)
	        	triads = triads/3;
	        
	        output.collect(key, new Text(String.valueOf(triads)));
        }
	}
	
	
	public static void main(String[] args) throws Exception {
		// Job 1
		JobConf conf = new JobConf(Triad_MapReduce.class);
	    conf.setJobName("triadCensusJob1");
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
	    JobConf conf2 = new JobConf(Triad_MapReduce.class);
	    conf2.setJobName("triadCensusJob2");
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
	    
//	    // Job 3
	    JobConf conf3 = new JobConf(Triad_MapReduce.class);
	    conf3.setJobName("triadCensusJob3");
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
