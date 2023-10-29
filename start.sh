#!/bin/bash

# get the root dir and set main class
PROJECT_DIR=$(pwd)
MAIN_CLASS="org.dbiir.harp.bootstrap.Bootstrap"

# set path to lib
LIB_DIR="$PROJECT_DIR/lib"
CLASSPATH="$LIB_DIR/*"

CONF_PATH="$PROJECT_DIR/conf"
ADDRESSES="0.0.0.0"

PORT=6603

# MAIN_CLASS="${MAIN_CLASS} ${PORT} ${CONF_PATH} ${ADDRESSES}"
CLASS_PATH=${CONF_PATH}:${CLASS_PATH}

# start
java -cp "$CLASSPATH" "$MAIN_CLASS" "$PORT" "$CONF_PATH" "$ADDRESSES" "$FORCE" "$@"
