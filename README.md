# storm-hdfs-spout
Storm spout using [HDFS INotify API](https://issues.apache.org/jira/browse/HDFS-6634) to produce tuples based on filesystem events.

## Requirements
- Admin access to HDFS
- Hadoop >= 2.6.0 

## Current functionality
Produces tuples in response to CLOSE event.
