SOURCES=$(wildcard *.clj)

./target/big-replicate-standalone.jar: $(SOURCES)
	echo $(SOURCES)
	lein uberjar
