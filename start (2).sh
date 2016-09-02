#!/bin/bash



shname=$0
if [ ${shname%/*} != ${shname#*/} ];
then
    cd ${shname%/*};
fi    
echo `pwd`
#datestr=`date +'%Y%m%d'`
datestr=$(date --date yesterday "+%Y%m%d")
logdate=$(date --date yesterday "+%Y-%m-%d")

logdir=/data/appdatas/cloud_behavior_go/downloaded



if ! test -d ./stat/cachingpackagesignquery/$datestr ;
then
    mkdir -p ./stat/cachingpackagesignquery/$datestr
    touch ./stat/cachingpackagesignquery/$datestr/caching_package_sign_query_top_path_stat_log.log
    gzip -S .gz.done ./stat/cachingpackagesignquery/$datestr/caching_package_sign_query_top_path_stat_log.log
fi

if ! test -d ./stat/appdirsignquery/$datestr ;
then
    mkdir -p ./stat/appdirsignquery/$datestr
    touch ./stat/appdirsignquery/$datestr/dir_sign_query_top_path_stat_log.log
    gzip -S .gz.done ./stat/appdirsignquery/$datestr/dir_sign_query_top_path_stat_log.log
fi

#read pip 
#./realtimeanalyzelog.goc appdirsignquery.pip appdirsignquery $datestr
#find $logdir/$datestr/appdirsignquery/ -name "appdirsignquery_*.log.gz.done" -exec zcat {} \; >> appdirsignquery.pip 
#echo "quit" >> appdirsignquery.pip

#./realtimeanalyzelog.goc cachingpackagesignquery.pip cachingpackagesignquery $datestr
#find $logdir/$datestr/cachingpackagesignquery/ -name "cachingpackagesignquery_*.log.gz.done" -exec zcat {} \; >> cachingpackagesignquery.pip 
#echo "quit" >> cachingpackagesignquery.pip


#walk dir
nohup ./realtimeanalyzelog.goc $logdir/$datestr/cachingpackagesignquery cachingpackagesignquery $logdate >cachingpackagesignquery_$datestr.log 2>&1&
nohup ./realtimeanalyzelog.goc $logdir/$datestr/appdirsignquery appdirsignquery $logdate >appdirsignquery_$datestr.log 2>&1&


