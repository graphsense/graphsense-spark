[![sbt test](https://github.com/graphsense/graphsense-ethereum-transformation/actions/workflows/sbt_test.yml/badge.svg)](https://github.com/graphsense/graphsense-ethereum-transformation/actions/workflows/sbt_test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# GraphSense Ethereum Transformation Pipeline

The GraphSense Transformation Pipeline reads raw block and transaction data,
which is ingested into [Apache Cassandra][apache-cassandra]
by the [graphsense-ethereum-etl][graphsense-ethereum-etl] component.
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

Download, install, and run [Apache Spark][apache-spark] (version 3.2.1)
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
`transaction`, `block`).
Check as follows

    cqlsh localhost
    cqlsh> USE eth_raw;
    cqlsh:btc_raw> DESCRIBE tables;

## Execute Transformation on localhost

Create the target keyspace for transformed data

    cqlsh -f scripts/schema_transformed.cql

Compile and test the implementation

    sbt test

Package the transformation pipeline

    sbt package

Run the transformation pipeline on localhost

    ./submit.sh

Check the running job using the local Spark UI at http://localhost:4040/jobs

# Submit on a standalone Spark Cluster

macOS only: make sure `gnu-getopt` is installed (`brew install gnu-getopt`).

Use the `submit.sh` script and specify the Spark master node
(e.g., `-s spark://SPARK_MASTER_IP:7077`) and other options:

```
./submit.sh -h
Usage: submit.sh [-h] [-m MEMORY_GB] [-c CASSANDRA_HOST] [-s SPARK_MASTER]
                 [--raw_keyspace RAW_KEYSPACE] [--tgt_keyspace TGT_KEYSPACE]
                 [--bucket_size BUCKET_SIZE]
```


# Contributions

Community contributions e.g. new features and bug fixes are very welcome. For both please create a pull request with the proposed changes. We will review as soon as possible. To avoid frustration and wasted work please contact us to discuss changes before you implement them. This is best done via an issue or our [discussion board](https://github.com/orgs/graphsense/discussions/).

Please make sure that the submitted code is always tested and properly formatted. 

Do not forget to format and test your code using 
```
sbt scalafmt && sbt test
```
before committing.

# Useful links
[graphsense-ethereum-etl]: https://github.com/graphsense/graphsense-ethereum-etl
[graphsense-dashboard]: https://github.com/graphsense/graphsense-dashboard
[graphsense-rest]: https://github.com/graphsense/graphsense-rest
[graphsense-setup]: https://github.com/graphsense/graphsense-setup
[java]: https://adoptopenjdk.net
[scala-lang]: https://www.scala-lang.org
[scala-sbt]: http://www.scala-sbt.org
[dsbulk]: https://github.com/datastax/dsbulk
[apache-spark]: https://spark.apache.org/downloads.html
[apache-cassandra]: http://cassandra.apache.org


