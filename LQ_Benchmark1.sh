#!/bin/sh
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -ea -Xmx1g -jar ./build/distributions/util.concurrent-1.0-shadow.jar ".*LQ_Benchmark1.*" -wi 5 -i 10 -f 0


