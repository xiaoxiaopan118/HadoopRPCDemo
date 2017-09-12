package cn.ctyun.bigdata.topic.rpc;

import java.io.IOException;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.ctyun.bigdata.topic.utils.GetProperties;

import org.apache.hadoop.ipc.Server;

public class JobUtilsServiceMain {
	private static Logger LOGGER = LoggerFactory.getLogger(JobUtilsServiceMain.class);
	public static String IPAddress = GetProperties.getPropertieValue("rpc.server.ip");
	public static int PORT = Integer.parseInt(GetProperties.getPropertieValue("rpc.server.port"));
    public static Server build;
    
	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException, InterruptedException{
		
		Builder builder = new RPC.Builder(new Configuration());
		builder.setBindAddress(IPAddress).setPort(PORT).setProtocol(JobUtilsService.class)
				.setInstance(new JobUtilsServiceImpl());
		build = builder.build();
		build.start();
		LOGGER.info("JobUtilsServiceMain---启动了---IPAddress:" + IPAddress + "--PORT:" + PORT);
		LOGGER.info("查看服务启动情况请使用：jps | grep JobUtilsServiceMain");
		LOGGER.info("关闭JobUtilsServiceMain服务请使用:jps | grep JobUtilsServiceMain |awk '{print $1}'|xargs kill -9");
		LOGGER.info("HadoopRPC--启动了!!!");
//		Thread.sleep(1000 * 10);
//		build.stop();
//		System.out.println("HadoopRPC---关闭了！！！");
	}

}
