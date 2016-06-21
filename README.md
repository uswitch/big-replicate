# Big Replicate
Replicates data between Google Cloud BigQuery projects. Currently focused on copying Google Analytics Premium BigQuery exported data between different projects.

## Usage

The tool currently expects the destination project to have a dataset (with the same name as the source) that already exists. It will look for any session tables that are missing from the destination dataset, and replicate the `--number` of most recent ones.

```
export GCLOUD_PROJECT="$SOURCE_PROJECT"
export GOOGLE_APPLICATION_CREDENTIALS="./service-account-key.json"

export SOURCE_PROJECT="some-project"
export DESTINATION_PROJECT="other-project"
export STAGING_BUCKET="gs://some-bucket"
export JVM_OPTS="-Dlogback.configurationFile=./logback.example.xml"

java $JVM_OPTS -jar big-replicate.jar \
  --source-project source-project-id \
  --destination-project destination-project-id \
  --google-cloud-bucket gs://staging-data-bucket \
  --datasets 98909919 \
  --number 30
```

Because only missing tables from the destination dataset are processed, tables will not be overwritten.

## Building

The tool is written in [Clojure](https://clojure.org) and requires [Leiningen](https://github.com/technomancy/leiningen).

```
$ make
```

## To Do

* The tool currently assumes its replicating only Google Analytics exported data. It would be nice to change this to allow a table regexp to be specified on the cli so its less specific to GA data.
* The tool also assumes its replicating data between projects (with datasets of the same name). It might be useful to be able to copy data across projects as well as datasets.
* Staging data is not automatically deleted once its been loaded into the destination table. 

## License

Copyright © 2016 uSwitch

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
