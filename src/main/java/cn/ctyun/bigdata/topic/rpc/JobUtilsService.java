package cn.ctyun.bigdata.topic.rpc;

import org.apache.hadoop.io.Text;

public interface JobUtilsService {
	public static final long versionID = 2422732438392142930L;

	public String startMyJob(String[] str);
	
	public boolean writeToFile(String outPathFile, Text text);

}