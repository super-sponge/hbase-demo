#! /usr/bin/env bash
##########################################################################
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
##########################################################################
#
# base.sh:  .
#
# This script is customized to work nice with inittab respawn.
#
##########################################################################

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`
SCRIPT=`basename ${PRG}`




#if [ ! -f "${HADOOP_BIN}" ]; then
#  echo "HADOOP_BIN must be set and point to the location of the hadoop script"
#  exit 1;
#fi

export APP_JAR_DIR=$BASEDIR/lib

function add_to_classpath() {
  dir=$1
  for f in $dir/*.jar; do
    EXT_CLASSPATH=${EXT_CLASSPATH}:$f;
  done

  export APP_CLASSPATH
}

add_to_classpath ${APP_JAR_DIR}


export APP_CONF_DIR="${BASEDIR}/conf"
export APP_CLASSPATH=${EXT_CLASSPATH}

#export JAVA=$JAVA_HOME/bin/java
#export JAVA_HEAP_MAX=-Xmx512m
