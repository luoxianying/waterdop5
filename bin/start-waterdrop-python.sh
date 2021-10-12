#!/bin/bash

# copy command line arguments
CMD_ARGUMENTS=$@

PARAMS=""
while (( "$#" )); do
  case "$1" in
    --master)
      MASTER=$2
      shift 2
      ;;

    --deploy-mode)
      DEPLOY_MODE=$2
      shift 2
      ;;

    --conf)
      CONF=$2
      shift 2
      ;;

    --driver-memory)
      DRIVER_MEMORY=$2
      shift 2
      ;;
    --executor-memory)
      EXECUTOR_MEMORY=$2
      shift 2
      ;;
    --num-executors)
      NUM_EXECUTORS=$2
      shift 2
      ;;
    --executor-core)
      EXECUTOR_CORES=$2
      shift 2
      ;;
    --config)
      CONFIG_FILE=$2
      shift 2
      ;;
    --) # end argument parsing
      shift
      break
      ;;

    # -*|--*=) # unsupported flags
    #  echo "Error: Unsupported flag $1" >&2
    #  exit 1
    #  ;;

    *) # preserve positional arguments
      PARAM="$PARAMS $1"
      shift
      ;;

  esac
done
# set positional arguments in their proper place
eval set -- "$PARAMS"

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=${BIN_DIR}/utils
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
LIB_DIR=${APP_DIR}/lib
#PLUGINS_DIR=${APP_DIR}/python_lib

DEFAULT_CONFIG=${CONF_DIR}/application.conf
CONFIG_FILE=${CONFIG_FILE:-$DEFAULT_CONFIG}

DEFAULT_DEPLOY_MODE=client
DEPLOY_MODE=${DEPLOY_MODE:-$DEFAULT_DEPLOY_MODE}

DEFAULT_DRIVER_MEMORY=1g
DRIVER_MEMORY=${DRIVER_MEMORY:-$DEFAULT_DRIVER_MEMORY}

DEFAULT_EXECUTOR_MEMORY=1g
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-$DEFAULT_EXECUTOR_MEMORY}

DEFAULT_NUM_EXECUTORS=1
NUM_EXECUTORS=${NUM_EXECUTORS:-$DEFAULT_NUM_EXECUTORS}

DEFAULT_EXECUTOR_CORES=2
EXECUTOR_CORES=${EXECUTOR_CORES:-$DEFAULT_EXECUTOR_CORES}

#source ${UTILS_DIR}/file.sh
#jarDependencies=$(listJarDependenciesOfPlugins ${PLUGINS_DIR})
#JarDepOpts=""
#if [ "$jarDependencies" != "" ]; then
#    JarDepOpts="--jars $jarDependencies"
#fi

exec su - hdfs -c "spark-submit --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --conf ${CONF} \
    --driver-memory ${DRIVER_MEMORY} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --num-executors ${NUM_EXECUTORS} \
    --executor-cores ${EXECUTOR_CORES} \
    --jars ${LIB_DIR}/spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar \
      ${CONFIG_FILE}"