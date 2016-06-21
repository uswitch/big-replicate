SOURCES=$(wildcard *.clj) $(wildcard src/uswitch/*.clj)

./target/big-replicate-standalone.jar: $(SOURCES)
	lein uberjar
