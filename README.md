[![Build Status](https://travis-ci.org/agoda-com/kafka-jdbc-connector.svg?branch=master)](https://travis-ci.org/agoda-com/kafka-jdbc-connector)
[![Gitter chat](https://badges.gitter.im/kafka-jdbc-connector/kafka-jdbc-connector.png)](https://gitter.im/kafka-jdbc-connector/Lobby)
[![License: Apache](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/agoda-com/kafka-jdbc-connector/blob/master/LICENSE.txt)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.agoda/kafka-jdbc-connector_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.agoda/kafka-jdbc-connector_2.11)
[![Codecov branch](https://img.shields.io/codecov/c/github/agoda-com/kafka-jdbc-connector/master.svg)]()

Kafka JDBC Connector
====================

*Simple way to copy data from relational databases into kafka.*

To copy data between Kafka and another system, users create a Connector for the system which they want to pull data from or push data to. Connectors come in two flavors: *SourceConnectors* to import data from another system and *SinkConnectors* to export data from Kafka to other datasources. This project is an implementation of *SourceConnector* which allows users to copy data from relational databases into Kafka topics. It provides a flexible way to keep logic of selecting next batch of records inside database stored procedures which is invoked from the connector tasks in each poll cycle.

Following are few advantages of using stored procedures

* Select only required columns to be returned as a record.
* Filtering / Grouping rows returned to connector with SQL.
* Changing logic of returned batch without reconfiguring connector.

Install
-------

build.sbt

```scala
libraryDependencies ++= Seq("com.agoda" %% "kafka-jdbc-connector" % "1.2.0")
```

Change Log
----------

Please refer to [Change Log](https://github.com/agoda-com/kafka-jdbc-connector/blob/master/CHANGE_LOG.md) for requirements, supported databases and project changes.

Examples
--------

Click [here](https://github.com/agoda-com/kafka-jdbc-connector-samples) for examples.

### Timestamp mode

Create a stored procedure in MSSQL database

```
create procedure [dbo].[cdc_table]
	@time datetime,
	@batch int
as
begin
   select top (@batch) *
   from        cdc.table_ct as a
   left join   cdc.lsn_time_mapping as b
   on          a._$start_lsn = b.start_lsn
   where       b.tran_end_time > @time
   order by    b.tran_end_time asc
end
```

Post the following configutation to Kafka Connect rest interface

```
{
	"name" : "cdc_timestamp",
	"config" : {
		"tasks.max": "1",
		"connector.class": "com.agoda.kafka.connector.jdbc.JdbcSourceConnector",
		"connection.url" : "jdbc:sqlserver://localhost:1433;user=sa;password=Passw0rd",
		"mode" : "timestamp",
		"stored-procedure.name" : "cdc_table",
		"topic" : "cdc-table-changelogs",
		"batch.max.rows.variable.name" : "batch",
		"timestamp.variable.name" : "time",
		"timestamp.field.name" : "tran_end_time"
	}
}
```

### Incrementing mode

Create a stored procedure in MSSQL database

```
create procedure [dbo].[cdc_table]
	@id int,
	@batch int
as
begin
   select top (@batch) *
   from        cdc.table_ct
   where       auto_incrementing_id > @id
   order by    auto_incrementing_id asc
end
```

Post the following configutation to Kafka Connect rest interface

```
{
	"name" : "cdc_incrementing",
	"config" : {
		"tasks.max": "1",
		"connector.class": "com.agoda.kafka.connector.jdbc.JdbcSourceConnector",
		"connection.url" : "jdbc:sqlserver://localhost:1433;user=sa;password=Passw0rd",
		"mode" : "incrementing",
		"stored-procedure.name" : "cdc_table",
		"topic" : "cdc-table-changelogs",
		"batch.max.rows.variable.name" : "batch",
		"incrementing.variable.name" : "id",
		"incrementing.field.name" : "auto_incrementing_id"
	}
}
```

### Timestamp + Incrementing mode

Create a stored procedure in MSSQL database

```
create procedure [dbo].[cdc_table]
	@time datetime,
	@id int,
	@batch int
as
begin
   select top (@batch) *
   from        cdc.table_ct as a
   left join   cdc.lsn_time_mapping as b
   on          a._$start_lsn = b.start_lsn
   where       b.tran_end_time > @time
   and         a.auto_incrementing_id > @id
   order by    b.tran_end_time, a.auto_incrementing_id asc
end
```

Post the following configutation to Kafka Connect rest interface

```
{
	"name" : "cdc_timestamp_incrementing",
	"config" : {
		"tasks.max": "1",
		"connector.class": "com.agoda.kafka.connector.jdbc.JdbcSourceConnector",
		"connection.url" : "jdbc:sqlserver://localhost:1433;user=sa;password=Passw0rd",
		"mode" : "timestamp+incrementing",
		"stored-procedure.name" : "cdc_table",
		"topic" : "cdc-table-changelogs",
		"batch.max.rows.variable.name" : "batch",
		"timestamp.variable.name" : "time",
		"timestamp.field.name" : "tran_end_time",
		"incrementing.variable.name" : "id",
		"incrementing.field.name" : "auto_incrementing_id"
	}
}
```

Contributing
------------

**Kafka JDBC Connector** is an open source project, and depends on its users to improve it. We are more than happy to find you interested in taking the project forward.

Kindly refer to the [Contribution Guidelines](https://github.com/agoda-com/kafka-jdbc-connector/blob/master/CONTRIBUTING.md) for detailed information.

Code of Conduct
---------------

Please refer to [Code of Conduct](https://github.com/agoda-com/kafka-jdbc-connector/blob/master/CODE_OF_CONDUCT.md) document.

License
-------

Kafka JDBC Connector is Open Source and available under the [Apache License, Version 2.0](https://github.com/agoda-com/kafka-jdbc-connector/blob/master/LICENSE.txt).
