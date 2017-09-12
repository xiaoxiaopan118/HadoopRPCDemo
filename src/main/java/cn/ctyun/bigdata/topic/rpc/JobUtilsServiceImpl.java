package cn.ctyun.bigdata.topic.rpc;

import org.apache.hadoop.io.Text;

import cn.ctyun.bigdata.topic.utils.StartShellLine;
import cn.ctyun.bigdata.topic.utils.WriteTextToFile;

public class JobUtilsServiceImpl implements JobUtilsService {

	/**
	 * 启动shell脚本
	 */
	public String startMyJob(String[] str){
		String string = "";
		try {
			string = StartShellLine.startShell(str);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return string;
	}

	/**
	 * 写text数据到outPathFile文件中
	 */
	public boolean writeToFile(String outPathFile, Text text) {
		boolean re = false;
		try {	
			re = WriteTextToFile.writeToFile(outPathFile,text);
		} catch (Exception e) {
			re = false;
		}
		return re;
	}
	
}