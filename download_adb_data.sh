#!/bin/bash
d=2017-01-01
while [ "$d" !=  2018-01-01 ]; do 
	if ! hdfs dfs -ls "/user/s1638696/flight_data/""$d"".tar.gz" 
	then
		if [ ! -e "$d".zip ]
		then
			if [ ! -e "$d".tar.gz ]
			then
				echo Missing file for "$d"
				wget http://history.adsbexchange.com/Aircraftlist.json/"$d".zip
				unzip "$d".zip -d "$d"
				tar -czvf "$d".tar.gz "$d"
				hdfs dfs -put "$d".tar.gz /user/s1638696/flight_data/"$d".tar.gz
				rm "$d".zip
				rm "$d".tar.gz
				rm -rf "$d"
			fi
		fi
	fi
  d=$(date -I -d "$d + 1 day")
done
