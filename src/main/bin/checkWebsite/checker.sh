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

# Website status checker. by ET (etcs.me)

WORKSPACE=/scripts/isOnline
# list of websites. each website in new line. leave an empty line in the end.
LISTFILE=$WORKSPACE/websites.lst
# Send mail in case of failure to. leave an empty line in the end.
EMAILLISTFILE=$WORKSPACE/emails.lst
EMAILTEXTFILE=$WORKSPACE/mail_text_template
# Temporary dir
TEMPDIR=$WORKSPACE/cache
# Temporary mail text
mailtextfile=mail_text
# log file
logfile=$TEMPDIR/request_log
temp_logfile=$TEMPDIR/request_log_temp

# `Quiet` is true when in crontab; show output when it's run manually from shell.
# Set THIS_IS_CRON=1 in the beginning of your crontab -e.
# else you will get the output to your email every time
if [ -n "$THIS_IS_CRON" ]; then QUIET=true; else QUIET=false; fi

function test {
  filename=$( echo $1 | cut -f1 -d"/" )
  if [ "$QUIET" = false ] ; then echo -n "$p "; fi
  # 1 means testing failed
  failed=1
  for i in {1..3} 
  do 
	response=$(curl -L --max-time 20 --trace-ascii $logfile --trace-time --write-out %{http_code} --silent --output /dev/null $1)
        echo "$i times: " >> $temp_logfile
        cat $logfile >> $temp_logfile
        
	if [ "$response" = 200 ] ; then
	  # website working
	  if [ "$QUIET" = false ] ; then
	    echo -n "$response "; echo -e "\e[32m[ok]\e[0m"
	  fi
	  # remove .temp file if exist 
	  if [ -f $TEMPDIR/$filename ]; then rm -f $TEMPDIR/$filename; fi
	  if [ -f $logfile ]; then rm -f $logfile; fi
          failed=0
          break;
        else
          # failed and sleep 5 seconds and retry
          echo "Sleep 5 seconds " >> $temp_logfile 
          sleep 5
          echo "Now retry  " >> $temp_logfile 
        fi
  done

  mv $temp_logfile $logfile
  if [ "$failed" -eq 1 ] ; then
       rm -f $TEMPDIR/$filename
       # rename the log file so that the file is kept for later inspection
       suffix=$(date +%s)
       mv $logfile "$logfile.$suffix"
       # website down
       if [ "$QUIET" = false ] ; then 
          echo -n "response is : $response "; 
          echo -e "\e[31m[DOWN]\e[0m"; 
       else 
          echo -n "$response " >> "$logfile.$suffix";
       fi
       if [ ! -f $TEMPDIR/$filename ]; then
          while read e; do
            echo "To:$e" > $TEMPDIR/$mailtextfile
            cat $EMAILTEXTFILE >> $TEMPDIR/$mailtextfile
            # using mail command
            /usr/sbin/ssmtp -vvv $e < $TEMPDIR/$mailtextfile 2>&1 | tee /tmp/abcdef
            #mail -s "$p WEBSITE DOWN" "$EMAIL"
          done < $EMAILLISTFILE
          echo > $TEMPDIR/$filename
       fi
  fi
}

# main loop
while read p; do
   test $p
done < $LISTFILE
