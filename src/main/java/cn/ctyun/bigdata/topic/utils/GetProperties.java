package cn.ctyun.bigdata.topic.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 * 获取配置文件信息
 * @author panlijie
 *
 */
public final class GetProperties {
	private static Properties prop = null;
	static{
		prop = new Properties();
		//设置properties文件路径,以src为根路径(application.properties 处于src根目录下)
		InputStream in = GetProperties.class.getResourceAsStream("/application.properties");
		try {
			prop.load(in);
		} catch (IOException e) {
			System.out.println("读取配置文件失败----(/application.properties)");
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据传入的key获取配置文件中对应的value值
	 * @param key
	 * @return value
	 */
	public static String getPropertieValue(String key){
		String value = "";
		value = prop.getProperty(key).trim();    
		return value;
	}
	/*public static void main(String[] args) {
		System.out.println(getPropertieValue("rpc.server.ip"));
	}*/
}