package com.cloudera.cdk.hbase.data.service;

import org.apache.log4j.Logger;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.RandomAccessDatasetRepository;


public abstract class AbstractHBaseService {

	protected static Logger logger;
	protected String hbaseUrl;
	protected RandomAccessDatasetRepository repo = null;

	public AbstractHBaseService() {
		super();
		logger = Logger.getLogger(this.getClass());
	}
	
	public AbstractHBaseService(String repoUrl) {
		this();
		this.setHbaseUrl(repoUrl);
	}
	
	public void setHbaseUrl(String hbaseUrl) {
		logger.debug("setting hbase url to: " + hbaseUrl);
		this.hbaseUrl = hbaseUrl;
		repo = DatasetRepositories.openRandomAccess(this.hbaseUrl);		
	}

}