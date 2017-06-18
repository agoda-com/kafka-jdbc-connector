[![Build Status](https://travis-ci.org/agoda-com/kafka-jdbc-connector.svg?branch=master)](https://travis-ci.org/agoda-com/kafka-jdbc-connector)
[![Gitter chat](https://badges.gitter.im/kafka-jdbc-connector/kafka-jdbc-connector.png)](https://gitter.im/kafka-jdbc-connector/Lobby)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/agoda-com/kafka-jdbc-connector/blob/master/LICENSE.txt)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.agoda/kafka-jdbc-connector_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.agoda/kafka-jdbc-connector_2.11)

Kafka JDBC Connector
====================

*Simple way to copy data from relational databases into kafka.*

Install
-------

build.sbt

```scala
libraryDependencies ++= Seq("com.agoda" %% "kafka-jdbc-connector" % "0.9.0.0")
```

Requirements
------------

* ***Scala*** version 2.11.* and 2.12.*
* ***Kafka*** version 0.9.0.0
* ***Kafka Connect*** version 0.9.0.0

Examples
--------

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

License
-------

Kafka JDBC Connector is Open Source and available under the [MIT License](https://github.com/agoda-com/kafka-jdbc-connector/blob/master/LICENSE.txt).

TODO
----

* Change logs
* Code of conduct
* Sample project
