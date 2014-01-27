hbase-core
==========

This project depends on Kite (https://github.com/kite-sdk/kite).

Prerequisties
-------------
- Hadoop client package from CDH
- HBase package from CDH

Getting Started
---------------
In hbase-core run the following:

mvn compile -Dmaven.test.skip=true

Delete existing hbase tables and metadata (only needed if you change avro schemas):
```delete-datasets.sh```

Create new datasets via Kite
```./create-datasets.sh```

Run MapReduce jobs, sample scripts are provided in *-mr.sh:
``` ./party-mr.sh```

The HBase tables should be populated at this point.
