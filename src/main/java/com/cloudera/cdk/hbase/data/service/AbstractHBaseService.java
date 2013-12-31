package com.cloudera.cdk.hbase.data.service;

import org.apache.log4j.Logger;

import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.RandomAccessDatasetRepository;

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