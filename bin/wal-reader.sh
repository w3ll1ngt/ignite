#!/usr/bin/env bash
if [ -n "${IGNITE_SCRIPT_STRICT_MODE:-}" ]
then
    set -o nounset
    set -o errexit
    set -o pipefail
    set -o errtrace
    set -o functrace
fi


#По ридеру - его нужно вынести куда-то в core модуль и написать скрипт для запуска.
# Возможно те параметры которые в него приходят - переделать,
#   чтобы идентичным образом с control.sh были (
#   чтобы не было зоопарка типа в одном месте --cache cacheName в другом месте cache=cacheName
#   Нужно проверить что точно есть флаг скрытия sensitive информации (он там точно есть, возможно даже тест есть, но я не смотрел),
#   иначе безопасность может не пустить эту утилиту.
#   Конкретно меня интересовал фильтр по pageId, он там тоже вроде есть
# А.Плеханов


# Stream SSO
# в идеале требуется достичь понимания,
# действительно ли эвикт не справляется и не успевает удалить вещи с expiryTime протухшим

# Да. Оптимизация в 17.5 от Максима Тимонина

# todo:
# check igniterouter.sh ignite.cdc

# index-reader todo check

#!/usr/bin/env bash
if [ -n "${IGNITE_SCRIPT_STRICT_MODE:-}" ]
then
    set -o nounset
    set -o errexit
    set -o pipefail
    set -o errtrace
    set -o functrace
fi

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# The utility for reading wal archives.
#

#
# Import common functions.
#
if [ "${IGNITE_HOME:-}" = "" ];
    then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else IGNITE_HOME_TMP=${IGNITE_HOME};
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh
source "${SCRIPTS_HOME}"/include/jvmdefaults.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh
. "${SCRIPTS_HOME}"/include/build-classpath.sh # Will be removed in the binary release.
CP="${IGNITE_LIBS}"

# Mac OS specific support to display correct name in the dock.
osname=$(uname)

if [ "${DOCK_OPTS:-}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Ignite Node"
fi

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
if [ -z "${CONTROL_JVM_OPTS:-}" ] ; then
    if "$JAVA" -version 2>&1 | grep -qE "1\.[7]\." ; then
        CONTROL_JVM_OPTS="-Xms256m -Xmx1g"
    else
        CONTROL_JVM_OPTS="-Xms256m -Xmx1g"
    fi
fi

# todo: think about it and check what was that
# Uncomment to enable experimental commands [--wal]
#
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -DIGNITE_ENABLE_EXPERIMENTAL_COMMAND=true"

#
# Uncomment if you get StackOverflowError.
# On 64 bit systems this value can be larger, e.g. -Xss16m
#
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -Xss4m"

#
# Assertions are disabled by default since version 3.5. todo check what is 3.5
# If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
#
ENABLE_ASSERTIONS="1"

#
# Set '-ea' options if assertions are enabled.
#
if [ "${ENABLE_ASSERTIONS:-}" = "1" ]; then
    CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -ea"
fi

#
# Set main class to start service (grid node by default).
#
if [ "${MAIN_CLASS:-}" = "" ]; then
    MAIN_CLASS=org.apache.ignite.internal.commandline.walreader.IgniteWalConverter
fi

#
# Final CONTROL_JVM_OPTS for Java 9+ compatibility
#
CONTROL_JVM_OPTS=$(getJavaSpecificOpts $version "$CONTROL_JVM_OPTS")

if [ -n "${JVM_OPTS}" ] ; then
  echo "JVM_OPTS environment variable is set, but will not be used. To pass JVM options use CONTROL_JVM_OPTS"
  echo "JVM_OPTS=${JVM_OPTS}"
fi

case $osname in
    Darwin*)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} "${DOCK_OPTS}" \
         -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" \
         -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
    *)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} \
         -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" \
         -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
esac
