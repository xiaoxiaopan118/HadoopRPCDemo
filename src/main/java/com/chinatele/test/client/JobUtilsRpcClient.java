package com.chinatele.test.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

public class JobUtilsRpcClient {

	public static int PORT = 0;
	public static String IPAddress = "";

	static {
		IPAddress = "";
		PORT = 8080;
	}

	/**
	 * RPC客户端启动远程服务器端脚本和传递参数
	 * 
	 * @param str
	 *            str[0]=脚本路径 str[1]=参数ID
	 * @return
	 * @throws IOException
	 */
	public static String startTheJobClient(String[] str) throws IOException {
		JobUtilsService proxy = RPC.getProxy(JobUtilsService.class, JobUtilsService.versionID,
				new InetSocketAddress(IPAddress, PORT), new Configuration());
		String returnstr = proxy.startMyJob(str);
		RPC.stopProxy(proxy);
		return returnstr;
	}

	/**
	 * 接收输入流向远程RPC服务器端写outPathFile文件 （仅支持txt文件）
	 * @param in
	 * @param outPathFile
	 * @return
	 */
	public static boolean writeStringToFile(InputStream in, String outPathFile) {
		BufferedReader reader;
		StringBuffer sb = new StringBuffer();
		try {
			reader = new BufferedReader(new InputStreamReader(in));
			String value = "";
			while ((value = reader.readLine()) != null) {
				sb.append(value + "\r\n");
			}
			// 读取结束
			reader.close();
			// 远程调用写入
			Text text = new Text(sb.toString());
			JobUtilsService proxy = RPC.getProxy(JobUtilsService.class, JobUtilsService.versionID,
					new InetSocketAddress(IPAddress, PORT), new Configuration());
			boolean bo = proxy.writeToFile(outPathFile, text);
			//关闭客户端代理
			RPC.stopProxy(proxy);
			return bo;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

}
