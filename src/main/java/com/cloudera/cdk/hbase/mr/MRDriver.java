package com.cloudera.cdk.hbase.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
//import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.PropertySource;

import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.data.RandomAccessDatasetRepository;
import com.cloudera.cdk.hbase.data.avro.Party;
import com.cloudera.cdk.hbase.data.util.PropertiesManager;

//@Configuration
//@EnableAutoConfiguration
//@ComponentScan
//@PropertySource("classpath:application.properties")
public class MRDriver {
	
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = Job.getInstance();
		//Configuration conf = job.getConfiguration();
		
		job.setJarByClass(MRDriver.class);
		job.setJobName("MR Driver");
		
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
	
		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setMapperClass(CommonToHbaseMapper.class);
		//job.setReducerClass(TopScoreReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setNumReduceTasks(0);
		
		job.waitForCompletion(true);
	}

	public static class CommonToHbaseMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		
		static Logger logger = Logger.getLogger(CommonToHbaseMapper.class);
		//Text outputValue = new Text();
		static NullWritable nullWritable = NullWritable.get();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\\|");
	        System.out.println("Number of fields: " + vals.length);
			System.out.println("Fields: " + vals[0] + "*" + vals[1] + "*" + vals[2] + "*" + vals[3] + "*" + vals[4]);
			
			String id = vals[0];
			String desc = vals[1];
			String type = vals[2];
			long startDttm = vals[3].trim().isEmpty() ? 0 : Long.parseLong(vals[3].trim());
			long endDttm = vals[4].trim().isEmpty() ? 0 : Long.parseLong(vals[4].trim());
			String statusCode = vals[5];
			String statusDesc = vals[6];
			String languageCode = vals[7];
			String eventId = vals[8];

		    RandomAccessDatasetRepository repo =
			        //DatasetRepositories.openRandomAccess(PropertiesManager.getProperty("hbase.url"));
	        		DatasetRepositories.openRandomAccess("repo:hbase:localhost.localdomain");
		    RandomAccessDataset<Party> parties = repo.load("party");
		    Party party = instantiateParty(id, desc, type, startDttm, endDttm, statusCode, statusDesc, languageCode, eventId);
		    logger.debug("Putting party: " + party.toString());
		    parties.put(party);
		    
			context.write(nullWritable, nullWritable);
		}
		
		  private static Party instantiateParty(
				  String id, String desc, String type, 
                  long startDttm, long endDttm, 
                  String statusCode, String statusDesc, 
                  String languageCode, String eventId) {
			  return Party.newBuilder()
					  .setId(id)
					  .setDesc(desc)
					  .setType(type)
					  .setStartDttm(startDttm)
					  .setEndDttm(endDttm)
					  .setStatusCode(statusCode)
					  .setStatusDesc(statusDesc)
					  .setLanguageCode(languageCode)
					  .setEventId(eventId)
					  .build();
		  }
	 }
}
