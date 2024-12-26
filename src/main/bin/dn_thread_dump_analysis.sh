#!/bin/bash
# Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 


if [ $# -eq 1 ]; then
    thread_dump_file="$1"
    echo $thread_dump_file
else
    echo "Your command line contains no arguments"
fi

num_threads=`grep "prio=" $thread_dump_file | wc -l`
echo "the number of threads: $num_threads"

num_client_workers=`grep "py-client-.*worker" $thread_dump_file | wc -l`
echo "the number of client workers: $num_client_workers"

num_service_workers=`grep "DataNodeService-request-worker" $thread_dump_file | wc -l`
echo "the number of dn service workers: $num_service_workers"

num_epoll_wait_threads=`grep "epollWait" $thread_dump_file | wc -l`
echo "the number of epoll wait threads: $num_epoll_wait_threads"

num_logdriver_threads=`grep LogDriver $thread_dump_file | wc -l`
echo "the number of log driver threads: $num_logdriver_threads"

num_statedriver_threads=`grep StateProcessing $thread_dump_file | wc -l`
echo "the number of state driver threads: $num_statedriver_threads"

num_runnable_threads=`grep RUNNABLE $thread_dump_file | wc -l`
echo "the number of runnable threads: $num_runnable_threads"

num_epoll_wait_threads_runnable=`grep -B 1 "epollWait" $thread_dump_file | grep RUNNABLE | wc -l`
echo "the number of epoll wait threads in the runnable state: $num_epoll_wait_threads_runnable"

num_logdriver_threads_runnable=`grep -A 1 LogDriver $thread_dump_file | grep RUNNABLE | wc -l`
echo "the number of log driver threads in the runnable: $num_logdriver_threads_runnable"

num_statedriver_threads_runnable=`grep -A 1 StateProcessing $thread_dump_file | grep RUNNABLE | wc -l`
echo "the number of state driver threads in the runnable: $num_statedriver_threads_runnable"

num_client_workers_runnable=`grep "py-client-.*worker" $thread_dump_file | grep RUNNABLE | wc -l`
echo "the number of client workers in the runnable : $num_client_workers_runnable"

num_service_workers_runnable=`grep "DataNodeService-request-worker" $thread_dump_file | grep RUNNABLE | wc -l`
echo "the number of dn service workers in the runnable : $num_service_workers_runnable"

