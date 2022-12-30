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

# absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..

java -server -Xms500m -Xmx1024m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xloggc:logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.utils.performance.test.PerformanceTestClient $*
