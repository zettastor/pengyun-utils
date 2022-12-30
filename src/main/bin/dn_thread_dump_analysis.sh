#!/bin/bash

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

