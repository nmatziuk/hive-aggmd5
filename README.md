# An Aggregate Version of Hive's md5() Function
This project was originally developed using Eclipse with Gradle.  To use it with Eclipse, simply clone the repository and import into Eclipse as a Gradle project.  Then use the Gradle tasks to build the jar file.

To use from Hive:

```
add jar file:///localpath/hive-aggmd5-1.0.0.jar;
set hive.query.result.fileformat=sequencefile;
CREATE TEMPORARY FUNCTION aggregate_md5 as 'com.nick.hiveutils.udf.AggregateMD5';

select aggregate_md5(concat(*)) md5hash from test_table;
```