#!/usr/bin/env bash
#
# Copyright (c) 2022. PengYunNetWork
#
# This program is free software: you can use, redistribute, and/or modify it
# under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
#  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
#  You should have received a copy of the GNU Affero General Public License along with
#  this program. If not, see <http://www.gnu.org/licenses/>.
#

# use simple netty client and server to test network
# you need do these below before test:
# 1. use command `ssh-copy-id` to avoid input password for logging onto server
# 2. change pengyun-utils/src/main/resources/log4j.properties, set root log level to WARN or higher
# 3. modify pengyun-utils/pom.xml comment out the plugin:`maven-assembly-plugin`, this will save about one minute compile time
# 4. compile all needed projects(include pengyun-utils). This script will not compile any code. Everytime you modify code, you need compile manually before run this script.
# After run script, you can use iftop command to monitor network

# if set true, script will kill client and server process and just exit
#only_kill="true"
only_kill="false"

heap_memory="10G"
direct_memory="20G"

java_opt="-noverify -server -Xms$heap_memory -Xmx$heap_memory -XX:MaxDirectMemorySize=$direct_memory -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:GCPauseIntervalMillis=500 -XX:ParallelGCThreads=10 -XX:+HeapDumpOnOutOfMemoryError"

# enable jmx metric
metric_port=10105
metric_params="-Dmetric.enable=true -Dmetric.enable.profiles=metric.jmx -Dcom.sun.management.jmxremote.port=$metric_port -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false"

java_opt="$java_opt $metric_params"

# enable yourkit
#java_opt="$java_opt -agentpath:/opt/yourkit/libyjpagent.so"

# server hosts where to upload jar files
client_ip="10.0.2.231"
server_ip="10.0.2.232"

# netty server listen host and port
server_listen_ip="172.16.10.232"
#server_listen_ip="172.16.1.209"
server_listen_port="5556"

# workspace at remote server
remote_workspace="/root/network_tune/simple_cp"

# remote path where to store jar files
remote_lib_path="$remote_workspace/lib"

# some special flag to pass to JVM, you can add by yourself
specific_opt=""

# class name
client_class_name="py.nettysetup.SimpleNettyClient"
server_class_name="py.nettysetup.SimpleNettyServer"

# gc log config
gc_opt="-Xloggc:$remote_workspace/gc.log -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+PrintReferenceGC -verbose:gc -XX:+UnlockDiagnosticVMOptions -XX:PrintSafepointStatisticsCount=1 -XX:+SafepointTimeout -XX:SafepointTimeoutDelay=2000 -XX:+G1SummarizeConcMark -XX:+G1SummarizeRSetStats -XX:G1SummarizeRSetStatsPeriod=1 -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=100M"
java_opt="$java_opt $gc_opt"

# client param[1] 
# listen_port

# client param[2]
write_request_length="$((8 * 1024))"

# client param[3]
unit_count_per_request="128" 
# client param[4]
iodepth="25"

# client param[5]
test_write="true"

# client param[6]
queue_capacity="192"

# export project's all dependent jar files
export_jar() {
    base="$(pwd)"

    rm -fr $base/target/lib 
    mvn dependency:copy-dependencies -DoutputDirectory=$base/target/lib

    cp $base/target/pengyun-utils-*.jar $base/target/lib/
}

# remote execute some command
remote_exec() {
    ip="$1"
    cmd="$2"

    ssh root@$ip "$cmd"
}

# remote kill process on server
kill_process() {
    ip="$1"
    flag="$2"

    cmd="ps -ef | grep $flag | grep -v grep | awk '{print \$2}' | xargs -r -n 1 kill -9 "
    remote_exec "$ip" "$cmd"

    #ssh root@$ip "ps -ef | grep $flag | grep -v grep | awk '{print \$2}' | xargs -r -n 1 kill -9 "
}

# kill client and server process
#kill_process "$client_ip" "$client_class_name"
#kill_process "$server_ip" "$server_class_name"

if [[ "X$only_kill" == "Xtrue" ]]; then
    echo "only kill process"
    exit 0
fi

# export needed jars
export_jar

# upload jar file
remote_exec "$client_ip" "mkdir -p $remote_lib_path"
remote_exec "$server_ip" "mkdir -p $remote_lib_path"
rsync -av --delete target/lib root@$client_ip:$remote_workspace
rsync -av --delete target/lib root@$server_ip:$remote_workspace

# start server process
echo "start server"
ssh root@$server_ip "nohup java -cp \"$remote_lib_path/*\" $java_opt $specific_opt $server_class_name $server_listen_ip $server_listen_port > $remote_workspace/server.log 2>&1 < /dev/null &"
sleep 3

# start client process
echo "start client"
ssh root@$client_ip "nohup java -cp \"$remote_lib_path/*\" $java_opt $specific_opt $client_class_name $server_listen_ip $server_listen_port $write_request_length $unit_count_per_request $iodepth $test_write $queue_capacity > $remote_workspace/client.log 2>&1 < /dev/null &"
