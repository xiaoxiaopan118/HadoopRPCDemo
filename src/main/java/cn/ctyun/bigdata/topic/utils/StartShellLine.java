package cn.ctyun.bigdata.topic.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class StartShellLine {

	/**
	 * str[0]脚本路径
	 * str[1]->str[n]传给脚本的参数
	 * @param str
	 * @return
	 * @throws InterruptedException
	 */
	public static String startShell(String[] str) throws InterruptedException {
		Process process = null;
		String returnstr = "";
		try {
			process = Runtime.getRuntime().exec(str);
			BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = "";
			while ((line = input.readLine()) != null) {
				System.out.println(line);
			}
			input.close();

			// 判断是否正常执行，如果正常结束，Process的waitFor()方法返回0
			int exitValue = process.waitFor();
			if (0 != exitValue) {
				returnstr = "failed";
			}
		} catch (IOException e) {
			e.printStackTrace();
			returnstr = e.toString();
		}
		return returnstr;
	}

	public static void main(String[] args) throws InterruptedException {
		startShell(args);
	}

}
