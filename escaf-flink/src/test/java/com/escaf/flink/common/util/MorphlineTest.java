package com.escaf.flink.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import com.codahale.metrics.SharedMetricRegistries;
import com.escaf.flink.common.function.MorphlineFunction.Collector;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class MorphlineTest {

	private MorphlineContext morphlineContext;

	private Command morphline;

	private Collector finalChid;

	@Test
	public void testTransLog() throws FileNotFoundException, IOException {

		String value = IOUtils.toString(new FileInputStream(new File("C:\\Users\\owner\\Desktop\\temp.conf")));
		Config config = ConfigFactory.parseString(value);
		Collector finalChid = new Collector();
		Command cmd = new Compiler().compile(config, morphlineContext, finalChid);

		Record record = new Record();
		String msg = "2018-03-25|801507|12|2018-04-17 17:05:08.478679|2018-04-17 17:05:08.483580|0.00|8020800|020777|999996|读取保函注销接口表失败[BHZX201803251590217],记录不存在|";
		record.put(Fields.ATTACHMENT_BODY, msg.getBytes());

		Notifications.notifyStartSession(cmd);
		System.out.println(cmd.process(record));
		record = finalChid.getRecords().get(0);
		GenericData.Record r = (org.apache.avro.generic.GenericData.Record) record.get("_attachment_body").get(0);

		System.out.println(r);
	}

	@Test
	public void testConf() {
		createMorphline("test2.conf");
		Record record = new Record();
		String msg = "Jun 10 04:42:51 INFO FLOW 1228";
		record.put(Fields.ATTACHMENT_BODY, msg.getBytes());

		Notifications.notifyStartSession(morphline);
		morphline.process(record);

		record = finalChid.getRecords().get(0);

		GenericData.Record r = (org.apache.avro.generic.GenericData.Record) record.get("_attachment_body").get(0);
		Object dataTime = r.get("datetime");
		Object age = r.get("age");
		System.out.println(dataTime.getClass() + " " + age.getClass() + " ");
		System.out.println("------------------------------------------");

		ListMultimap<String, Object> list = record.getFields();
		Map<String, Collection<Object>> map = list.asMap();
		Set<Entry<String, Collection<Object>>> set = map.entrySet();
		Iterator<Entry<String, Collection<Object>>> it = set.iterator();

		while (it.hasNext()) {
			Entry<String, Collection<Object>> en = it.next();

			String key = en.getKey();
			Collection<Object> c = en.getValue();
			Object o = c.toArray()[0];

			System.out.println(key + "  " + (o.getClass()) + " " + o);

		}

	}

	@Before
	public void setUp() {
		FaultTolerance faultTolerance = new FaultTolerance(false, false);

		morphlineContext = new MorphlineContext.Builder().setExceptionHandler(faultTolerance)
				.setMetricRegistry(SharedMetricRegistries.getOrCreate("testId")).build();
		finalChid = new Collector();
	}

	public void createMorphline(String path) {

		FaultTolerance faultTolerance = new FaultTolerance(false, false);

		morphlineContext = new MorphlineContext.Builder().setExceptionHandler(faultTolerance)
				.setMetricRegistry(SharedMetricRegistries.getOrCreate("testId")).build();
		finalChid = new Collector();
		Config override = ConfigFactory.empty();
		morphline = new Compiler().compile(
				new File("F:\\workspace\\flink-eclipse0\\escaf-flink\\src\\test\\resources\\" + path), "morphline1",
				morphlineContext, finalChid, override);

	}

}
