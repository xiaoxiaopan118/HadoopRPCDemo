package cn.ctyun.bigdata.topic.schedule;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.ctyun.bigdata.topic.model.TaskTools;

/**
 * 
 * @author panlijie
 *
 */
public class ScheduleAllJobs {

	private static Logger LOGGER = LoggerFactory.getLogger(ScheduleAllJobs.class);

	public static void main(String[] args) throws SQLException {
//		args = new String[] { "Task_HFmsxQ" };
		if (args.length != 1) {
			return;
		}
		
		String taskid = args[0];

		List<TaskTools> listTools = GetMysqlUtil.getTaskToosId(taskid);
		// task任务表task_conf中task_repeat_qty次数+1
		taskConfRepeatAdd(taskid);
		/**
		 * 执行状态标识 0:未执行，1:正在执行，2:执行成功，3:执行失败
		 */
		// 写入任务状态表task_status运行状态
		taskStatusNewRecord(taskid);
		// 开始对所有任务进行调度
		boolean jobsSuccess = startScheduleTools(taskid, listTools);
		if (jobsSuccess) {
			// 更新task_status中执行状态为成功或失败
			updateTaskStatusRecord(taskid,"2");
		} else {
			// 任务失败更改为失败状态
			updateTaskStatusRecord(taskid,"3");
		}

	}

	/**
	 * 更新任务表中任务的执行状态
	 * @param taskid
	 * @param string
	 */
	private static void updateTaskStatusRecord(String taskid, String status) {
		StartAndMonitorJob.updateTaskStatusRecord(taskid, status);
	}

	/**
	 * 新插入任务状态表task_status
	 * 
	 * @param taskid
	 * 
	 */
	private static void taskStatusNewRecord(String taskid) {
		StartAndMonitorJob.taskStatusNewRecord(taskid);
	}

	/**
	 * task任务表task_repeat_qty次数+1
	 * 
	 * @param taskid
	 */
	private static void taskConfRepeatAdd(String taskid) {
		StartAndMonitorJob.taskConfRepeatAdd(taskid);
	}

	private static boolean startScheduleTools(String taskid, List<TaskTools> listTools) throws SQLException {
		LOGGER.info("-----待执行job的总个数：" + listTools.size() + "------");
		boolean jobsSuccess = false;

		/**
		 * 循环三种类型的工具任务 三种工具类型toolfunction 1.数据挖掘型工具 2.求交集类型工具 3.提取手机属性工具
		 */
		for (int toolfunction = 1; toolfunction <= 3; toolfunction++) {
			Set<String> set = new HashSet<String>();
			// 将需要执行的任务类别放到队列中
			for (TaskTools taskTools : listTools) {
				if (String.valueOf(toolfunction).equals(taskTools.getToolFunction())) {
					set.add(taskTools.getToolId());
				}
			}
			// 开始对set队列中的job做执行
			if (!set.isEmpty()) {
				// 开始进行调度任务
				jobsSuccess = preStartSetJobs(taskid, set, toolfunction, listTools);
			}
			// 调度失败
			if (!jobsSuccess) {
				break;
			}
		}
		return jobsSuccess;
	}

	/**
	 * 执行任务入口
	 * 
	 * @param taskid
	 * @param set
	 * @param toolfunction
	 * @param listTools
	 * @return
	 * @throws SQLException
	 */
	private static boolean preStartSetJobs(String taskid, Set<String> set, int toolfunction, List<TaskTools> listTools)
			throws SQLException {
		boolean re = true;
		// 循环并行执行job
		for (String toolid : set) {
			LOGGER.info("----开始预加载job参数---taskid和toolid:" + taskid + ":" + toolid + " -------");
			boolean isContinue = StartAndMonitorJob.preStartJob(taskid, toolid, toolfunction, listTools);
			if (isContinue) {
				continue;
			} else {
				return false;
			}
		}
		// 检查并行任务所有的执行状态，都成功才返回true
		try {
			re = StartAndMonitorJob.getIsSuccessd(taskid, set);
		} catch (InterruptedException e) {
			re = false;
			e.printStackTrace();
		}
		return re;
	}

}
