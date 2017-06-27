#!/bin/bash

VM_OPTS="-Xmx2048m -Xms512m -XX:+UseG1GC"

folder=$(dirname $0)/..

pushd $folder > /dev/null

MYCLASSPATH="."
for JAR in lib/*.jar
do
    MYCLASSPATH=$MYCLASSPATH:$JAR
done

java -server $VM_OPTS -cp $MYCLASSPATH com.yesup.fun.$1
