export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/spark/bin:/opt/spark/sbin:/usr/lib/HBase/bin:/usr/lib/hive/bin:/usr/lib/sqoop/bin:/usr/lib/hadoop/sbin:/usr/lib/hadoop/bin:/usr/lib/mahout/bin:/usr/lib/pig/bin" &&
  hdfs dfs -test -d /data && hdfs dfs -rm -r /data
hdfs dfs -copyFromLocal data /data &&
  spark-submit loppronostic_2.12-0.1.1-SNAPSHOT.jar
