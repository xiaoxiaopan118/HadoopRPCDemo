package cn.ctyun.bigdata.topic.model;


/**
 * 工具配置实体类
 * @author panlijie
 *
 */
public class TaskTools{
	
	//工具唯一id uuid生成
	private String toolId;
	//工具功能
	private String toolFunction;
	
	public String getToolId() {
		return toolId;
	}
	public void setToolId(String toolId) {
		this.toolId = toolId;
	}
	public String getToolFunction() {
		return toolFunction;
	}
	public void setToolFunction(String toolFunction) {
		this.toolFunction = toolFunction;
	}
	
}
