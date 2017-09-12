#!/bin/sh
#-------------------------------------------------------------------------------------------------------------
#该脚本为测试调用jar包中的某一个方法执行路径下的脚本的例子
#-------------------------------------------------------------------------------------------------------------

shell_path=/home/proripc/data/panlijie/program/data/test.sh

java -classpath jobUtilsServer-0.0.1-SNAPSHOT-jar-with-dependencies.jar cn.ctyun.bigdata.topic.utils.StartShellLine ${shell_path}