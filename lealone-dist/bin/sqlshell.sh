#!/bin/sh

# Copyright Lealone Database Group.
# Licensed under the Server Side Public License, v 1.
# Initial Developer: zhh

if [ "x$LEALONE_HOME" = "x" ]; then
    LEALONE_HOME="`dirname "$0"`/.."
fi

if [ "x$JAVA_HOME" = "x" ]; then
    echo JAVA_HOME environment variable must be set!
    exit 1;
fi

LEALONE_MAIN=org.lealone.main.Shell

JAVA_OPTS=-Xms10M
JAVA_OPTS="$JAVA_OPTS -Dlealone.logdir=$LEALONE_HOME/logs"

CLASSPATH=$LEALONE_HOME/conf:$LEALONE_HOME/lib/*

LEALONE_PARAMS=$@

"$JAVA_HOME/bin/java" $JAVA_OPTS -cp $CLASSPATH $LEALONE_MAIN $LEALONE_PARAMS
