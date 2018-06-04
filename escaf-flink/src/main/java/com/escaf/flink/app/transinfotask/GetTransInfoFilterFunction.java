package com.escaf.flink.app.transinfotask;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * 这个类的功能可以使用Morphline的配置将其替代掉
 * 
 * @author HANLIN
 *
 */
@Deprecated
public class GetTransInfoFilterFunction implements FilterFunction<Map<String,Object>> {

	private static final long serialVersionUID = 1L;

	private static final String FILTER_LOG_TRANDE_NAME = "c.l.n.c.c.c.n.NcbsTrade";

	private static final String FILTER_LOG_MESSAGE_NAME = "c.l.n.c.c.c.n.p.NcbsMessage";

	private static final int CLAZZ_TRANDE_LOG_ROWNUM = 234;

	private static final int CLAZZ_TRANDE_MESSAGE_REQUEST_ROWNUM = 190;

	private static final int CLAZZ_TRANDE_CALL_ROWNUM = 406;

	private static final int CLAZZ_TRANDE_MESSAGE_RESPONSE_ROWNUM = 204;

	private static final int CLAZZ_TRANDE_END_RESPONSE_ROWNUM = 423;

	@Override
	public boolean filter(Map<String,Object> value) throws Exception {

		// XXX 需要把这个数据过滤的功能实现放在Morphline里面去做，尽量把 Morphline的ETL的效果做出来
		// 现在这里做出一个初步的版本

		if (value == null) {
			return false;
		}

		String clazzInfo = (String) value.get("className");

		if (clazzInfo == null) {
			return false;
		}

		int logRowNum = Integer.parseInt(value.get("rowNum").toString());
		if ((clazzInfo.equals(FILTER_LOG_MESSAGE_NAME) || clazzInfo.equals(FILTER_LOG_TRANDE_NAME))
				&& (logRowNum == CLAZZ_TRANDE_CALL_ROWNUM || logRowNum == CLAZZ_TRANDE_END_RESPONSE_ROWNUM
						|| logRowNum == CLAZZ_TRANDE_LOG_ROWNUM || logRowNum == CLAZZ_TRANDE_MESSAGE_REQUEST_ROWNUM
						|| logRowNum == CLAZZ_TRANDE_MESSAGE_RESPONSE_ROWNUM)) {

			return true;
		}

		return false;
	}
}