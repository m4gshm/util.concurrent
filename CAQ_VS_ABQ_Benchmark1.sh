#!/bin/sh
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -ea -Xmx1g -jar ./build/distributions/util.concurrent-0.3-B-shadow.jar ".*(_CAQ|ABQ)_Benchmark1.*" -wi 5 -i 10 -f 0


