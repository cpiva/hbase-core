package com.cloudera.cdk.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepository;
import com.cloudera.cdk.hbase.data.avro.Party;
import com.cloudera.cdk.hbase.data.avro.PartyAgreement;

public class PartyDriver {
	
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = Job.getInstance();
		//Configuration conf = job.getConfiguration();
		
		job.setJarByClass(PartyDriver.class);
		job.setJobName("Party Driver");
		
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
		static NullWritable nullWritable = NullWritable.get();
		
		private Configuration conf = null;
		//private HTablePool tablePool = null;
		private HBaseDatasetRepository repo = null;

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			//if (tablePool != null)
			//	tablePool.close();
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			conf = HBaseConfiguration.create();
			//tablePool = new HTablePool(conf, 5);
			repo = new HBaseDatasetRepository.Builder().configuration(conf).build();			
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\\|");
	        System.out.println("Number of fields: " + vals.length);
			
			String id = vals[0].trim();
			String desc = vals[1].trim();
			String type = vals[2].trim();
			Long startDttm = vals[3].trim().isEmpty() ? 0L : Long.parseLong(vals[3].trim());
			Long endDttm = vals[4].trim().isEmpty() ? 0L : Long.parseLong(vals[4].trim());
			String statusCode = vals[5].trim();
			String languageCode = vals[6].trim();
			String eventId = vals[7].trim();

			RandomAccessDataset<Party> table = null;
			try {
			 table = repo.load("party");
			 Party party = instantiateParty(id, desc, type, startDttm, endDttm, statusCode, languageCode, eventId);
			 table.put(party);

			/*
			HTableInterface table = null;
			try {
				table = tablePool.getTable("party");
				Put put = new Put(Bytes.toBytes(id));
				
				//put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
				if (!desc.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("desc"), Bytes.toBytes(desc));
				}
				if (!type.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("type"), Bytes.toBytes(type));
				}
				if (startDttm != null) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("start_dttm"), Bytes.toBytes(startDttm.longValue()));
				}
				if (endDttm != null) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("end_dttm"), Bytes.toBytes(endDttm.longValue()));
				}
				if (!statusCode.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("status_code"), Bytes.toBytes(statusCode));
				}
				if (!languageCode.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("language_code"), Bytes.toBytes(languageCode));
				}
				if (!eventId.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("event_id"), Bytes.toBytes(eventId));
				}
				
				table.put(put);
				*/
			} catch (Exception e) {
				System.out.println("HBase exception message: " + e.getMessage());
				e.printStackTrace(System.out);
			} finally {
				//if (table != null) {
				//	table.close();
				//}
			}
		    
			context.write(nullWritable, nullWritable);
		}
		
		private static Party instantiateParty(
				           String id, String desc, String type, 
				                   long startDttm, long endDttm, 
				                   String statusCode, 
				                   String languageCode, String eventId) {
				         return Party.newBuilder()
				             .setId(id)
				             .setDesc(desc)
				             .setType(type)
				             .setStartDttm(startDttm)
				             .setEndDttm(endDttm)
				             .setStatusCode(statusCode)
				             .setLanguageCode(languageCode)
				             .setEventId(eventId)
				             .build();
		}
		
	 }
}
