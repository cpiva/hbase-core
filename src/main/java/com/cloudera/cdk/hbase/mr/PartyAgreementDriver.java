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
import com.cloudera.cdk.hbase.data.avro.PartyAgreement;

public class PartyAgreementDriver {
	
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = Job.getInstance();
		//Configuration conf = job.getConfiguration();
		
		job.setJarByClass(PartyAgreementDriver.class);
		job.setJobName("PartyAgreement Driver");
		
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
			repo = new HBaseDatasetRepository.Builder().configuration(conf).build();
			//tablePool = new HTablePool(conf, 5);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\\|");
	        System.out.println("Number of fields: " + vals.length);
			
			String partyId = vals[0].trim();
			String agreementId = vals[1].trim();
			String role = vals[2].trim();
			String valu = vals[3].trim();

			RandomAccessDataset<PartyAgreement> table = null;
			try {
			 table = repo.load("party_agreement");
			 PartyAgreement partyAgreement = instantiatePartyAgreement(partyId, agreementId, role, valu);
			 table.put(partyAgreement);
			/*
			HTableInterface table = null;
			try {
				table = tablePool.getTable("party_agreement");
				Put put = new Put(Bytes.toBytes(getKey(partyId, agreementId)));
				
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
				if (!agreementId.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("agreement_id"), Bytes.toBytes(agreementId));
				}
				if (!role.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("role"), Bytes.toBytes(role));
				}
				if (!valu.isEmpty()) {
					put.add(Bytes.toBytes("_s"), Bytes.toBytes("value"), Bytes.toBytes(valu));
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
		
		private String getKey(String partyId, String agreementId) {
			StringBuilder sb = new StringBuilder(partyId);
			sb.append("_");
			sb.append(agreementId);
			return sb.toString();
		}

		private static PartyAgreement instantiatePartyAgreement(
				           String partyId, String agreementId, String role, String value) {
				         return PartyAgreement.newBuilder()
				             .setPartyId(partyId)
				             .setAgreementId(agreementId)
				             .setRole(role)
				             .setValue(value)
				             .build();
		}

	 }
}
