#!/bin/sh
#-------------------------------------------------------------------------------------------------------------
#该脚本为测试调用jar包中的某一个方法执行路径下的脚本的例子
#-------------------------------------------------------------------------------------------------------------

baseDir=/home/proripc/data/bdse-tour/bdse-job-Utils-Server
log_day=`date +%Y%m%d`
main=cn.ctyun.bigdata.topic.rpc.JobUtilsServiceMain

num=`jps | grep JobUtilsServiceMain |awk '{print $1}'| wc -l`
#echo ${num}
#echo ${log_day}
if [ ${num} -gt 0 ]; then
	echo "--数量:${num}---RPCServer服务已启动！！！----"
else
nohup java -classpath ${baseDir}/jobUtilsServer-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${main} >> ${baseDir}/${log_day}.log 2>&1 &
fi
##nohup java -classpath ${baseDir}/jobUtilsServer-0.0.1-SNAPSHOT-jar-with-dependencies.jar cn.ctyun.bigdata.topic.rpc.JobUtilsServiceMain >> ${baseDir}/${log_day}.log &