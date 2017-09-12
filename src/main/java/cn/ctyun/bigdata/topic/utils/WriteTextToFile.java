package cn.ctyun.bigdata.topic.utils;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;

public class WriteTextToFile {

	public static boolean writeToFile(String outPathFile, Text text){
		boolean re = false;
		try {
			FileUtils.writeStringToFile(new File(outPathFile), text.toString(), false);
			re = true;
		} catch (IOException e) {
			e.printStackTrace();
			re = false;
		}
		return re;
	}
}
