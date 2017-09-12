package com.chinatele.test.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.junit.Test;

public class JobRpcTest {

	public static int PORT = 0;
	public static String IPAddress = "";

	static {
		IPAddress = "";
		PORT = 8080;
	}

	@Test
	public void test1() throws IOException {
		JobUtilsService proxy = RPC.getProxy(JobUtilsService.class, JobUtilsService.versionID, new InetSocketAddress(IPAddress, PORT),
				new Configuration());
		String path = "/home/proripc/jobUtils/job/start.sh";
		String[] str = new String[] { path, "参数ID" };
		String result2 = proxy.startMyJob(str);
		System.out.println(result2);
	}

	@Test
	public void test2() {
			try {
				InputStream  in =  new FileInputStream("C:\\Users\\panlijie\\Desktop\\aaaa.txt");
				JobUtilsRpcClient.writeStringToFile(in, "C:\\Users\\panlijie\\Desktop\\bbbb.txt");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
	
	}
}
