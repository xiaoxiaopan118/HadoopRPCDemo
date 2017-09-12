package cn.ctyun.bigdata.topic.schedule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ctyun.bigdata.topic.model.TaskTools;
import cn.ctyun.bigdata.topic.utils.JdbcUtils;

/**
 * 
 * @author panlijie
 *
 */
public class GetMysqlUtil {
	private static Logger LOGGER = LoggerFactory.getLogger(GetMysqlUtil.class);

	/**
	 * 根据工具id查询工具启动脚本路径
	 * 
	 * @param id
	 * @return sh_path
	 * @throws SQLException
	 */
	public static String getToolShPath(String id) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		String sql2 = "SELECT run_sh_path FROM task_tools WHERE tool_id= ? ";
		List<Object> params = new ArrayList<Object>();
		params.add(id);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql2, params);
		String sh_path = "";
		if (!list.isEmpty()) {
			sh_path = list.get(0).get("run_sh_path").toString();
		} else {
			LOGGER.info("-------传入id:" + id + " 未找到对应工具启动脚本！！！-----");
		}
		jdbcUtils.releaseConn();
		return sh_path;
	}

	/**
	 * 通过taskid获取该任务下按照run_num排序的所有工具id
	 * 
	 * @param taskid
	 * @return
	 * @throws SQLException
	 */
	public static List<TaskTools> getTaskToosId(String taskid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		String sql2 = "SELECT tj.tool_id,tt.tool_function FROM task_jobs tj "
				+ "JOIN task_tools tt on tj.tool_id = tt.tool_id "
				+ "WHERE task_id = ? ORDER BY tt.tool_function";
		List<Object> params = new ArrayList<Object>();
		params.add(taskid);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql2, params);
		ArrayList<TaskTools> tool_ids = new ArrayList<TaskTools>();

		if (!list.isEmpty()) {
			for (int i = 0; i < list.size(); i++) {
				TaskTools taskTools = new TaskTools();
				taskTools.setToolFunction(list.get(i).get("tool_function").toString());
				taskTools.setToolId(list.get(i).get("tool_id").toString());
				tool_ids.add(taskTools);
			}
		} else {
			LOGGER.info("-------传入taskid:" + taskid + " 未找到对应任务编号！！！-----");
		}
		jdbcUtils.releaseConn();
		return tool_ids;
	}

	/**
	 * 根据任务id和工具id查询该job对应的执行状态 0:未执行，1:正在执行，2:执行成功，3:执行失败
	 * 
	 * @param taskid
	 * @param toolid
	 * @return
	 * @throws SQLException
	 */
	public static String checkIsSuccess(String taskid, String toolid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		String sql2 = "SELECT run_state FROM tools_status WHERE task_id= ? AND tool_id= ? "
				+ "AND task_run_no=(SELECT task_repeat_qty FROM task_conf WHERE task_id = ?)";
		List<Object> params = new ArrayList<Object>();
		params.add(taskid);
		params.add(toolid);
		params.add(taskid);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql2, params);
		String the_state = "";
		if (!list.isEmpty()) {
			the_state = list.get(0).get("run_state").toString();
		} else {
			LOGGER.info("-------传入taskid:" + taskid + ",toolid:" + toolid + " 未找到对应任务编号！！！-----");
		}
		jdbcUtils.releaseConn();
		return the_state;
	}

	/**
	 * 通过taskid和toolid查询该job的配置参数
	 * 
	 * @param taskid
	 * @param toolid
	 * @return Map<String, String>
	 * @throws SQLException
	 */
	public static Map<String, String> getParamsByTaskisToolid(String taskid, String toolid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();

		// String sql2 ="SELECT pm_name,pm_value FROM task_parameters tp
		// JOIN(SELECT pg_id FROM task_jobs WHERE task_id= ? AND tool_id= ?) pg
		// ON tp.pg_id=pg.pg_id";

		String sql2 = "SELECT pm_name,pm_value FROM task_parameters tp JOIN ( "
				+ "SELECT pg_id FROM task_jobs WHERE task_id= ? AND tool_id= ? ) pg ON tp.pg_id=pg.pg_id JOIN ( "
				+ "SELECT tool_pm_name,tool_pm_num FROM task_tools_pm WHERE tool_id= ?) tt "
				+ "ON tt.tool_pm_name=tp.pm_name ORDER BY tt.tool_pm_num ";
		List<Object> params = new ArrayList<Object>();
		params.add(taskid);
		params.add(toolid);
		params.add(toolid);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql2, params);
		Map<String, String> paramMap = new LinkedHashMap<String, String>();
		if (!list.isEmpty()) {
			for (int i = 0; i < list.size(); i++) {
				String key = list.get(i).get("pm_name").toString();
				String value = list.get(i).get("pm_value").toString();
				paramMap.put(key, value);
			}
		} else {
			LOGGER.info("-------传入taskid:" + taskid + ",toolid:" + toolid + " 未找到对应参数信息！！！-----");
		}
		jdbcUtils.releaseConn();
		return paramMap;
	}

	/**
	 * 根据taskid和toolid修改运行状态
	 * @param taskid
	 * @param toolid
	 * @param state
	 * @return
	 * @throws SQLException
	 */
	@Deprecated
	public static boolean updateRunState(String taskid, String toolid, String state) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		String sql = "UPDATE task_status SET run_state = ?,start_time= ? WHERE task_id= ? AND tool_id= ?";
		List<Object> params = new ArrayList<Object>();
		params.add(state);
		params.add(System.currentTimeMillis());
		params.add(taskid);
		params.add(toolid);
		boolean flag = jdbcUtils.updateByPreparedStatement(sql, params);
		jdbcUtils.releaseConn();
		return flag;
	}

	/**
	 * 对该任务表的执行次数字段+1
	 * 并插入运行状态表运行状态数据
	 * 0:未执行，1:正在执行，2:执行成功，3:执行失败
	 * @param taskid
	 * @param toolid
	 * @param state
	 * @return
	 * @throws SQLException
	 */
	public static void insertRunState(String taskid, String toolid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		//查询任务执行的次数
		String sql1 = "SELECT task_repeat_qty FROM task_conf WHERE task_id = ? ";
		//对任务表执行次数+1
//		String sql2 = "UPDATE task_status SET task_run_no=task_run_no + 1 WHERE task_id= ? AND tool_id= ? ";
		//插入任务运行表运行状态
		String sql3 = "INSERT INTO tools_status (task_id,tool_id,task_run_no,run_state,start_time) VALUES(?,?,?,?,?)";
		List<Object> params = new ArrayList<Object>();
		params.add(taskid);
//		params.add(toolid);
		//对任务表执行次数+1
//		jdbcUtils.updateByPreparedStatement(sql2, params);
		//查询执行次数
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql1, params);
		int task_run_no = 1;
		if (!list.isEmpty()) {
			task_run_no = Integer.parseInt(list.get(0).get("task_repeat_qty").toString());
		}else{
			LOGGER.info("-------传入taskid:" + taskid + ",toolid:" + toolid + " 未找到task_run_no任务运行状态信息！！！-----");
		}
		//插入任务运行表运行状态
		List<Object> params2 = new ArrayList<Object>();
		params2.add(taskid);
		params2.add(toolid);
		params2.add(task_run_no);
		params2.add("1");
		params2.add(System.currentTimeMillis());
		jdbcUtils.updateByPreparedStatement(sql3, params2);
		
		jdbcUtils.releaseConn();
		
	}
	
	/**
	 * 获取第一个job的输入和输出路径
	 * 
	 * @param toolid
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, String> getTheInAndOutPath(String toolid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		String sql2 = "SELECT inpath,outpath FROM task_tools WHERE tool_id= ?";
		List<Object> params = new ArrayList<Object>();
		params.add(toolid);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql2, params);
		Map<String, String> paramMap = new HashMap<String, String>();
		if (!list.isEmpty()) {
			String inpath = list.get(0).get("inpath").toString();
			paramMap.put("inpath", inpath);
			String outpath = list.get(0).get("outpath").toString();
			paramMap.put("outpath", outpath);
		} else {
			LOGGER.info("-------传入toolid:" + toolid + " 未找到对应参数信息！！！-----");
		}
		jdbcUtils.releaseConn();
		return paramMap;
	}

	/**
	 * 查询上一个job的输出路径为这个job的输入路径
	 * @param prevtoolid
	 * @param toolid
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, String> getPrevInAndOutPath(String prevtoolid, String toolid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		Map<String, String> paramMap = new HashMap<String, String>();
		//输入路径查询上一个tool的输出路径
		String sql1 = "SELECT outpath FROM task_tools WHERE tool_id= ?";
		List<Object> params = new ArrayList<Object>();
		params.add(prevtoolid);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql1, params);
		if (!list.isEmpty()) {
			String inpath = list.get(0).get("outpath").toString();
			paramMap.put("inpath", inpath);
		}else {
			LOGGER.info("-------传入toolid:" + prevtoolid + " 未找到对应参数信息！！！-----");
		}
			
		List<Object> params2 = new ArrayList<Object>();
		params2.add(toolid);
		List<Map<String, Object>> list1 = jdbcUtils.findModeResult(sql1, params2);
		if (!list.isEmpty()) {
			String outpath = list1.get(0).get("outpath").toString();
			paramMap.put("outpath", outpath);
		}else {
			LOGGER.info("-------传入toolid:" + toolid + " 未找到对应参数信息！！！-----");
		}
		jdbcUtils.releaseConn();
		return paramMap;
	}

	
	/**
	 * task任务表task_repeat_qty次数+1
	 * @param taskid
	 * @return
	 * @throws SQLException
	 */
	public static boolean updateTaskConfRepeat(String taskid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		String sql = "UPDATE task_conf SET task_repeat_qty = task_repeat_qty + 1 WHERE task_id= ?";
		List<Object> params = new ArrayList<Object>();
		params.add(taskid);
		boolean flag = jdbcUtils.updateByPreparedStatement(sql, params);
		jdbcUtils.releaseConn();
		return flag;
	}

	/**
	 * 新插入任务状态表task_status
	 * 执行状态标识
	 * 0:未执行，1:正在执行，2:执行成功，3:执行失败
	 * @param taskid
	 */
	public static boolean taskStatusNewRecord(String taskid) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		int run_num = 0;
		//输入路径查询上一个tool的输出路径
		String sql1 = "SELECT task_repeat_qty FROM task_conf WHERE task_id= ?";
		List<Object> params1 = new ArrayList<Object>();
		params1.add(taskid);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql1, params1);
		if (!list.isEmpty()) {
			run_num = Integer.parseInt(list.get(0).get("task_repeat_qty").toString());
		}else {
			LOGGER.info("-------传入taskid:" + taskid + " task_conf表中未找到对应参数信息！！！-----");
		}
		
		String sql = "INSERT INTO task_status (task_id, task_run_no, run_state, start_time) VALUES(?,?,?,?)";
		List<Object> params = new ArrayList<Object>();
		params.add(taskid);
		params.add(run_num);
		params.add("1");
		params.add(System.currentTimeMillis());
		boolean flag = jdbcUtils.updateByPreparedStatement(sql, params);
		jdbcUtils.releaseConn();
		return flag;
		
	}

	/**
	 * 更新任务状态表task_status的执行状态
	 * 执行状态标识
	 * 0:未执行，1:正在执行，2:执行成功，3:执行失败
	 * @param taskid
	 * @param status
	 * @return
	 * @throws SQLException
	 */
	public static boolean updateTaskStatusRecord(String taskid, String status) throws SQLException {
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();
		int run_num = 0;
		//输入路径查询上一个tool的输出路径
		String sql1 = "SELECT task_repeat_qty FROM task_conf WHERE task_id= ?";
		List<Object> params1 = new ArrayList<Object>();
		params1.add(taskid);
		List<Map<String, Object>> list = jdbcUtils.findModeResult(sql1, params1);
		if (!list.isEmpty()) {
			run_num = Integer.parseInt(list.get(0).get("task_repeat_qty").toString());
		}else {
			LOGGER.info("-------传入taskid:" + taskid + " task_conf表中未找到对应参数信息！！！-----");
		}
		String sql = "UPDATE task_status SET finish_time = ?,run_state = ? WHERE task_id= ? AND task_run_no= ? ";
		List<Object> params = new ArrayList<Object>();
		params.add(System.currentTimeMillis());
		params.add(status);
		params.add(taskid);
		params.add(run_num);
		boolean flag = jdbcUtils.updateByPreparedStatement(sql, params);
		jdbcUtils.releaseConn();
		return flag;
	}


}
