package com.cloudera.cdk.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import com.cloudera.cdk.hbase.data.avro.Agreement;

public class AgreementDriver {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance();
		// Configuration conf = job.getConfiguration();

		job.setJarByClass(AgreementDriver.class);
		job.setJobName("Agreement Driver");

		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),
				args).getRemainingArgs();

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setMapperClass(CommonToHbaseMapper.class);
		// job.setReducerClass(TopScoreReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
	}

	public static class CommonToHbaseMapper extends
			Mapper<LongWritable, Text, NullWritable, NullWritable> {

		static Logger logger = Logger.getLogger(CommonToHbaseMapper.class);
		static NullWritable NULL_WRITABLE = NullWritable.get();

		private Configuration conf = null;
		private HBaseDatasetRepository repo = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			conf = HBaseConfiguration.create();
			repo = new HBaseDatasetRepository.Builder().configuration(conf)
					.build();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\\|");
			logger.debug("Number of fields: " + vals.length);

			String id = vals[0].trim();
			String desc = vals[1].trim();
			String lobCode = vals[2].trim();
			String reasonCode = vals[3].trim();
			String description = vals[4].trim();
			Long startDttm = vals[5].trim().isEmpty() ? null : Long
					.parseLong(vals[5].trim());
			Long endDttm = vals[6].trim().isEmpty() ? null : Long
					.parseLong(vals[6].trim());
			String typeCode = vals[7].trim();
			String currencyCode = vals[8].trim();
			// String statusCode = vals[9].trim();
			String eventId = vals[9].trim();

			RandomAccessDataset<Agreement> table = null;

			table = repo.load("agreement");
			Agreement agreement = instantiateAgreement(id, desc, lobCode,
					reasonCode, description, startDttm, endDttm, typeCode,
					currencyCode, eventId);
			table.put(agreement);

			context.write(NULL_WRITABLE, NULL_WRITABLE);
		}

		private static Agreement instantiateAgreement(String id, String desc,
				String lobCode, String reasonCode, String description,
				long startDttm, long endDttm, String typeCode,
				String currencyCode, String eventId) {
			return Agreement.newBuilder().setId(id).setDesc(desc)
					.setLobCode(lobCode).setReasonCode(reasonCode)
					.setDescription(description).setStartDttm(startDttm)
					.setEndDttm(endDttm).setTypeCode(typeCode)
					.setCurrencyCode(currencyCode).setEventId(eventId).build();
		}

	}
}
