# quasar-destination-postgres [![Build Status](https://travis-ci.org/slamdata/quasar-destination-postgres.svg?branch=master)](https://travis-ci.org/slamdata/quasar-destination-postgres) [![Bintray](https://img.shields.io/bintray/v/slamdata-inc/maven-public/quasar-destination-postgres.svg)](https://bintray.com/slamdata-inc/maven-public/quasar-destination-postgres) [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.slamdata" %% "quasar-destination-postgres" % <version>
```

## Configuration

```json
{
  "connectionUri": String
  [, "schema": String]
}
```

* `connectionUri` (REQUIRED): A Postgres [connection URL](https://jdbc.postgresql.org/documentation/head/connect.html) **without the leading `jdbc:`**. Example `postgresql://localhost:5432/db?user=alice&password=secret`.
* `schema` (OPTIONAL): The name of the schema to use for all database objects, uses the server default if omitted.
