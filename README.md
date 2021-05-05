[![sbt test](https://github.com/graphsense/graphsense-ethereum-transformation/actions/workflows/sbt_test.yml/badge.svg)](https://github.com/graphsense/graphsense-ethereum-transformation/actions/workflows/sbt_test.yml)

# GraphSense Ethereum Transformation Pipeline

The GraphSense Transformation Pipeline reads raw block and transaction data,
which is ingested into [Apache Cassandra][apache-cassandra]
by the [graphsense-ethereum-etl][graphsense-ethereum-etl] component, and
attribution tags provided by [graphsense-tagpacks][graphsense-tagpacks].
The transformation pipeline computes an address graph and de-normalized views
using [Apache Spark][apache-spark], which are again stored in Cassandra.

Access to computed de-normalized views is subsequently provided by the
[GraphSense REST][graphsense-rest] interface, which is used by the
[graphsense-dashboard][graphsense-dashboard] component.

This component is implemented in [Scala][scala-lang] using
[Apache Spark][apache-spark].

## Local Development Environment Setup

### Prerequisites

Make sure [Java 8][java] and [sbt >= 1.0][scala-sbt] is installed:

    java -version
    sbt about

Download, install, and run [Apache Spark][apache-spark] (version 2.4.7)
in `$SPARK_HOME`:

    $SPARK_HOME/sbin/start-master.sh

Download, install, and run [Apache Cassandra][apache-cassandra]
(version >= 3.11) in `$CASSANDRA_HOME`

    $CASSANDRA_HOME/bin/cassandra -f

### Ingest Raw Block Data

Download und extract the [DataStax Bulk Loader][dsbulk], add the `bin`
directory your `$PATH` variable, and ingest raw test data using

    scripts/dsbulk_load.sh

This should create a keyspace `eth_raw` (tables `exchange_rates`,
`transaction`, `block`) and `tagpacks` (table `address_tag_by_address`).
Check as follows

    cqlsh localhost
    cqlsh> USE eth_raw;
    cqlsh:btc_raw> DESCRIBE tables;
    cqlsh:btc_raw> USE tagpacks;
    cqlsh:tagpacks> DESCRIBE tables;

## Execute Transformation Locally

Create the target keyspace for transformed data

    cqlsh -f scripts/schema_transformed.cql

Compile and test the implementation

    sbt test

Package the transformation pipeline

    sbt package

Run the transformation pipeline on localhost

    ./submit.sh

macOS only: make sure `gnu-getopt` is installed `brew install gnu-getopt`

Check the running job using the local Spark UI at http://localhost:4040/jobs

# Submit on a standalone Spark Cluster

Use the `submit.sh` script and specify the Spark master node
(e.g., `-s spark://SPARK_MASTER_IP:7077`) and other options:

```
./submit.sh -h
Usage: submit.sh [-h] [-m MEMORY_GB] [-c CASSANDRA_HOST] [-s SPARK_MASTER]
                 [--src_keyspace RAW_KEYSPACE] [--tag_keyspace TAG_KEYSPACE]
                 [--tgt_keyspace TGT_KEYSPACE] [--bucket_size BUCKET_SIZE]
```

# Submit to an external standalone Spark Cluster using Docker

See the [GraphSense Setup][graphsense-setup] component, i.e., the README
file and the `transformation` subdirectory.


[graphsense-ethereum-etl]: https://github.com/graphsense/graphsense-ethereum-etl
[graphsense-tagpacks]: https://github.com/graphsense/graphsense-tagpacks
[graphsense-dashboard]: https://github.com/graphsense/graphsense-dashboard
[graphsense-rest]: https://github.com/graphsense/graphsense-rest
[graphsense-setup]: https://github.com/graphsense/graphsense-setup
[java]: https://adoptopenjdk.net
[scala-lang]: https://www.scala-lang.org
[scala-sbt]: http://www.scala-sbt.org
[dsbulk]: https://github.com/datastax/dsbulk
[apache-spark]: https://spark.apache.org/downloads.html
[apache-cassandra]: http://cassandra.apache.org
