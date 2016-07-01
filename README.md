# Big Replicate
Replicates data between Google Cloud BigQuery projects. Currently focused on copying Google Analytics Premium BigQuery exported data between different projects.

[![CircleCI](https://circleci.com/gh/uswitch/big-replicate.svg?style=svg)](https://circleci.com/gh/uswitch/big-replicate)

## Usage

The tool currently expects the destination project to have a dataset (with the same name as the source) that already exists. It will look for any session tables that are missing from the destination dataset, and replicate the `--number` of most recent ones.

```
export GCLOUD_PROJECT="source-project-id"
export GOOGLE_APPLICATION_CREDENTIALS="./service-account-key.json"

export JVM_OPTS="-Dlogback.configurationFile=./logback.example.xml"

java $JVM_OPTS -jar big-replicate.jar \
  uswitch.big_replicate.sync \
  --source-project source-project-id \
  --destination-project destination-project-id \
  --google-cloud-bucket gs://staging-data-bucket \
  --datasets 98909919 \
  --number 30
```

Because only missing tables from the destination dataset are processed tables will not be overwritten. 

In the example above it will look for all `ga_sessions_` tables that don't exist in the destination dataset (but do in the source). Of those, it sorts them lexicographically and in reverse order before processing `30`. This results in the last 30 days of missing days being loaded. 

`big-replicate` will run multiple extract and loads concurrently- this is currently set to the number of available processors (as reported by the JVM runtime). You can override this with the `--number-of-agents` flag.

## Building

The tool is written in [Clojure](https://clojure.org) and requires [Leiningen](https://github.com/technomancy/leiningen).

```
$ make
```

## License

Copyright © 2016 uSwitch

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
