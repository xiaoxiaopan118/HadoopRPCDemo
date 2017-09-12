package cn.ctyun.bigdata.topic.schedule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.ctyun.bigdata.topic.model.TaskTools;
import cn.ctyun.bigdata.topic.utils.GetProperties;
import cn.ctyun.bigdata.topic.utils.StartShellLine;

/**
 * 
 * @author panlijie
 *
 */
public class StartAndMonitorJob {
	private static Logger LOGGER = LoggerFactory.getLogger(StartAndMonitorJob.class);
	// 循环检查次数
	private static long LOOPNUM = Long.parseLong(GetProperties.getPropertieValue("loop.num"));
	// 循环检查间隔时间
	private static long LOOPMILLIS = Long.parseLong(GetProperties.getPropertieValue("loop.second")) * 1000;
	/*
	 * private String taskid; private String toolid;
	 * 
	 * public StartAndMonitorJob(String taskid, String toolid) { this.taskid =
	 * taskid; this.toolid = toolid; }
	 */

	/**
	 * 启动对应工具的脚本组装参数信息 run_state 0:未执行，1:正在执行，2:执行成功，3:执行失败
	 * 
	 * @param outpath
	 * @param inpath
	 */
	public static boolean startJobSh(List<String> list) {
		String[] array = new String[list.size()];
		list.toArray(array);
		try {
			LOGGER.info("------开始启动工具的脚本,传入参数:" + list.toString() + "-----");
			StartShellLine.startShell(array);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * 检查mysql库中的标记时候改变为已执行完成
	 * 
	 * @return
	 * @throws InterruptedException
	 * @throws SQLException
	 */
	public static boolean getIsSuccessd(String taskid, Set<String> set) throws InterruptedException, SQLException {
		// set转list避免remove的时候报错 java.util.ConcurrentModificationException
		ArrayList<String> list = new ArrayList<String>();
		for (String str : set) {
			list.add(str);
		}
		boolean flag = false;
		// 循环检查任务执行状态
		for (int i = 0; i <= LOOPNUM; i++) {
			flag = false;
			Thread.sleep(LOOPMILLIS);
			Iterator<String> iterator = list.iterator();
			while (iterator.hasNext()) {
				String toolid = iterator.next();
				String the_state = GetMysqlUtil.checkIsSuccess(taskid, toolid);
				LOGGER.info("------第" + (i + 1) + "次检查job是否执行完成,taskid和tooid:" + taskid + "," + toolid + " 结果="
						+ the_state + " -----");
				// 执行状态:0:未执行，1:正在执行，2:执行成功，3:执行失败
				if (the_state.equals("2")) {
					flag = true;
					iterator.remove();
					continue;
				} else if (the_state.equals("3")) {
					// 执行失败的任务移除队列
					flag = false;
					iterator.remove();
					break;
				}
			}
			// 队列为空和返回结果为true则执行结束不再循环查询状态
			if (list.isEmpty()) {
				break;
			}
		}
		return flag;
	}

	/**
	 * 执行脚本之前的参数组装
	 * 
	 * @param taskid
	 * @param toolid
	 * @param toolfunction
	 * @param listTools
	 * @return
	 * @throws SQLException
	 */
	public static boolean preStartJob(String taskid, String toolid, int toolfunction, List<TaskTools> listTools)
			throws SQLException {
		boolean flag = false;
		/**
		 * 三种工具类型toolfunction 1.数据挖掘型工具 2.求交集类型工具 3.提取手机属性工具
		 */
		// 求交集和提取手机属性工具查找前面工具的输出路径
		if (toolfunction != 1) {
			Set<String> set = new HashSet<String>();
			// 将需要执行的任务类别放到队列中
			for (TaskTools taskTools : listTools) {
				if (String.valueOf(toolfunction - 1).equals(taskTools.getToolFunction())) {
					set.add(taskTools.getToolId());
				}
			}
			// 在set中存储前面job的tooid,他们的输出路径即为该job的输入路径
			flag = preStartOtherJobs(taskid, toolid, set);
		} else {
			flag = preStartWaJueJobs(taskid, toolid);
		}

		return flag;
	}

	/**
	 * 非挖掘程序的参数准备
	 * 
	 * @param taskid
	 * @param toolid
	 * @param set
	 * @return
	 * @throws SQLException
	 */
	private static boolean preStartOtherJobs(String taskid, String toolid, Set<String> set) throws SQLException {
		String inpath = "";
		for (String tid : set) {
			Map<String, String> path = GetMysqlUtil.getTheInAndOutPath(tid);
			if (!path.isEmpty()) {
				String outpath = path.get("outpath");
				if (!outpath.endsWith("/")) {
					outpath = outpath + "/";
				}
				inpath = inpath + outpath + taskid + "/" + tid + "/" + ",";
			}
		}
		if (!inpath.equals("")) {
			inpath = inpath.substring(0, inpath.length() - 1);
		}
		Map<String, String> out = GetMysqlUtil.getTheInAndOutPath(toolid);
		if (!out.isEmpty()) {
			String outpath = out.get("outpath");
			if (!outpath.endsWith("/")) {
				outpath = outpath + "/";
			}
			outpath = outpath + taskid + "/" + toolid + "/";
			return preStartPublicParam(taskid, toolid, inpath, outpath);
		}
		return false;
	}

	/**
	 * 组装公共参数以外的参数
	 * 
	 * @param taskid
	 * @param toolid
	 * @param inpath
	 * @param outpath
	 * @return
	 */
	private static boolean preStartPublicParam(String taskid, String toolid, String inpath, String outpath)
			throws SQLException {
		String sh_path = GetMysqlUtil.getToolShPath(toolid);
		// 开始组装所需要的参数列表
		Map<String, String> paramMap = GetMysqlUtil.getParamsByTaskisToolid(taskid, toolid);
		ArrayList<String> list = new ArrayList<String>();
		list.add(sh_path);
		list.add(taskid);
		list.add(toolid);
		list.add(inpath);
		list.add(outpath);
		// 参数列表不为空才执行否则不执行
		if (!paramMap.isEmpty()) {
			for (String key : paramMap.keySet()) {
				list.add(paramMap.get(key));
			}
		}
		// 启动脚本执行job任务
		boolean isStart = startJobSh(list);
		if (isStart) {
			/**
			 * 提交任务后对相关表字段进行添加或修改 0:未执行，1:正在执行，2:执行成功，3:执行失败
			 */
			GetMysqlUtil.insertRunState(taskid, toolid);
			return true;
		}
		return false;
	}

	/**
	 * 挖掘程序的参数准备
	 * 
	 * @param taskid
	 * @param toolid
	 * @return
	 * @throws SQLException
	 */
	private static boolean preStartWaJueJobs(String taskid, String toolid) throws SQLException {
		Map<String, String> path = GetMysqlUtil.getTheInAndOutPath(toolid);
		if (!path.isEmpty()) {
			String inpath = path.get("inpath");
			String outpath = path.get("outpath");
			if (!inpath.endsWith("/")) {
				inpath = inpath + "/";
			}
			if (!outpath.endsWith("/")) {
				outpath = outpath + "/";
			}
			outpath = outpath + taskid + "/" + toolid + "/";
			return preStartPublicParam(taskid, toolid, inpath, outpath);
		}
		return false;
	}

	/**
	 * task任务表task_repeat_qty次数+1
	 * 
	 * @param taskid
	 */
	public static void taskConfRepeatAdd(String taskid) {
		try {
			GetMysqlUtil.updateTaskConfRepeat(taskid);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 新插入任务状态表task_status
	 * 
	 * @param taskid
	 */
	public static void taskStatusNewRecord(String taskid) {
		try {
			GetMysqlUtil.taskStatusNewRecord(taskid);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 更新task_status执行状态
	 * @param taskid
	 * @param status
	 */
	public static void updateTaskStatusRecord(String taskid, String status) {
		try {
			GetMysqlUtil.updateTaskStatusRecord(taskid,status);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
