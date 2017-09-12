package com.chinatele.test;

import java.lang.reflect.Method;
import java.util.Arrays;

public class TestReflect {

	public static void main(String[] args) throws Exception {
		args = new String[] { "cn.ctyun.bigdata.topic.rpc.JobUtilsServiceMain" };
		Class<?> clazz = Class.forName(args[0]);
		String[] copyOfRange = Arrays.copyOfRange(args, 1, args.length);
		Method method = clazz.getMethod("main", String[].class);
		method.invoke(null, (Object) copyOfRange);
	}
}
