package project;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Project {

	// global variables to keep track of number of
	// records and the delay
	static enum companyCounter { amazon, walmart, bestbuy, costco, macys};

	public static class ProjectMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private String twitterText;
		private Map<String, Integer> companyLocationMap;

		// Initialize the hashmap in the setup
		protected void setup(Context context){
			companyLocationMap = new HashMap<String, Integer>();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			if(isValidJson(value.toString()))
			{
				JsonParser parser = new JsonParser();
				JsonObject obj =  (JsonObject) parser.parse(value.toString());
				JsonObject userObj = (JsonObject) obj.get("user");

				twitterText = obj.get("text").toString().toUpperCase();

				// Checking for Amazon.
				if(twitterText.contains("AMAZON")){
					context.getCounter(companyCounter.amazon).increment(1);
					if(!userObj.get("location").isJsonNull()){
						genericFunction("AMAZON", getLocation(userObj.get("location").toString().toUpperCase()));
					}
				}

				// Checking for BestBuy.
				if(twitterText.contains("BESTBUY") || twitterText.contains("BEST BUY")){
					context.getCounter(companyCounter.bestbuy).increment(1);
					if(!userObj.get("location").isJsonNull()){
						genericFunction("BESTBUY", getLocation(userObj.get("location").toString().toUpperCase()));
					}
				}

				// Checking for Walmart.
				if(twitterText.contains("WALMART") || twitterText.contains("WAL-MART")){
					context.getCounter(companyCounter.walmart).increment(1);
					if(!userObj.get("location").isJsonNull()){
						genericFunction("WALMART", getLocation(userObj.get("location").toString().toUpperCase()));
					}
				}

				// Checking for Costco.
				if(twitterText.contains("COSTCO")){
					context.getCounter(companyCounter.costco).increment(1);
					if(!userObj.get("location").isJsonNull()){
						genericFunction("COSTCO", getLocation(userObj.get("location").toString().toUpperCase()));
					}
				}

				// Checking for Macys.
				if(twitterText.contains("MACYS") || twitterText.contains("MACY'S")){
					context.getCounter(companyCounter.macys).increment(1);
					if(!userObj.get("location").isJsonNull()){
						genericFunction("MACYS", getLocation(userObj.get("location").toString().toUpperCase()));
					}
				}
			}
		}

		// Function which checks whether the JSON is correct or not
		public boolean isValidJson(String value){
			try {
				JsonParser parser = new JsonParser();
				JsonObject obj =  (JsonObject) parser.parse(value.toString());
				return true;
			} catch(Exception e) {
				return false;
			}
		}

		// Generic function that increases the count of that company in HashMap
		// If that company is not present then creates it with count 1
		public void genericFunction(String company, String location){
			if(!location.equalsIgnoreCase("NONE")){
				if(companyLocationMap.containsKey(company+","+location)){
					Integer localCount = companyLocationMap.get(company+","+location);
					localCount = localCount + 1;
					companyLocationMap.put(company+","+location, localCount);
				}
				else{
					companyLocationMap.put(company+","+location, 1);
				}
			}
		}

		// Function which returns the location information based on the
		// location comparision from the acutal data.
		public String getLocation(String loc){
			String localLoc = "NONE";
			if(loc.contains("BOSTON")){
				localLoc = "BOSTON";
			}
			if(loc.contains("MIAMI")){
				localLoc = "MIAMI";
			}
			if(loc.contains("CHICAGO")){
				localLoc = "CHICAGO";
			}
			if(loc.contains("SAN FRANCISCO") || loc.contains("SANFRANCISCO")){
				localLoc = "SAN FRANCISCO";
			}
			if(loc.contains("NEW YORK") || loc.contains("NEWYORK")){
				localLoc = "NEW YORK";
			}
			if(loc.contains("LAS VEGAS") || loc.contains("LASVEGAS")){
				localLoc = "LAS VEGAS";
			}
			if(loc.contains("HOUSTON")){
				localLoc = "HOUSTON";
			}
			if(loc.contains("LOS ANGELES") || loc.contains("LOSANGELES")){
				localLoc = "LOS ANGELES";
			}
			if(loc.contains("AUSTIN")){
				localLoc = "AUSTIN";
			}
			return localLoc;
		}

		// Emitting the keys in the cleanup
		protected void cleanup(Context context) throws IOException, InterruptedException{
			Iterator<String> keySetIterator = companyLocationMap.keySet().iterator();
			while(keySetIterator.hasNext()){
				String localKey = keySetIterator.next();
				Text localText = new Text(localKey);
				IntWritable localCount = new IntWritable(companyLocationMap.get(localKey));
				context.write(localText, localCount);
			}
			// clearing the hashmap
			companyLocationMap.clear();
		}

	}

	// Partitioner: based on the company corresponding the records is passed
	// to the partitioner
	public static class ProjectPartitioner extends Partitioner<Text, IntWritable>{
		@Override
		public int getPartition(Text key, IntWritable value, int numberOfReducer) {
			String company = key.toString().split(",")[0];
			if(company.equalsIgnoreCase("AMAZON")){
				return 0;
			}
			else if(company.equalsIgnoreCase("BESTBUY")){
				return 1;
			}
			else if(company.equalsIgnoreCase("WALMART")){
				return 2;
			}
			else if(company.equalsIgnoreCase("COSTCO")){
				return 3;
			}
			else if(company.equalsIgnoreCase("MACYS")){
				return 4;
			}
			else {
				return 5;
			}
		}
	}

	// Reducer: does the local sum based on the key
	public static class ProjectReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Project <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Project");
		job.setJarByClass(Project.class);
		job.setMapperClass(ProjectMapper.class);
		job.setPartitionerClass(ProjectPartitioner.class);
		job.setReducerClass(ProjectReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(5);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		// Printing the global variable values
		if(job.waitForCompletion(true)){
			Double amazon = Double.valueOf(job.getCounters().findCounter(companyCounter.amazon).getValue());
			Double bestBuy = Double.valueOf(job.getCounters().findCounter(companyCounter.bestbuy).getValue());
			Double walmart = Double.valueOf(job.getCounters().findCounter(companyCounter.walmart).getValue());
			Double costco = Double.valueOf(job.getCounters().findCounter(companyCounter.costco).getValue());
			Double macys = Double.valueOf(job.getCounters().findCounter(companyCounter.macys).getValue());

			System.out.println("\n---------------------------------");
			System.out.println("\nAmazon : "+amazon);
			System.out.println("\nBestBuy : "+bestBuy);
			System.out.println("\nWalmart : "+walmart);
			System.out.println("\nCostco : "+costco);
			System.out.println("\nMacys : "+macys);
			System.out.println("---------------------------------\n");
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
	}
}
