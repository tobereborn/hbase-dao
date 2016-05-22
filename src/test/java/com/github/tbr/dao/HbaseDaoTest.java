package com.github.tbr.dao;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class HbaseDaoTest {
	private static final DateFormat SDF = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
	private static final byte[] TABLE_NAME = Bytes.toBytes("test_log_full");
	private static final byte[][] FAMILIES = new byte[][] { Bytes.toBytes("f"), Bytes.toBytes("l") };
	private HBaseTestingUtility utility;
	private HBaseDao dao;

	static {
		SDF.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	@Before
	public void setUp() throws Exception {
		this.utility = new HBaseTestingUtility();
		utility.startMiniCluster();
	}

	@After
	public void tearDown() throws Exception {
		if (utility != null) {
			utility.shutdownMiniCluster();
		}
	}

	@Test
	public void testUpdateAndGetFirstLogTime() throws ParseException, IOException {
		int id = 123456;
		String action = "tnx";
		String dateStr1 = "2015-05-01 10:00:00";
		String dateStr2 = "2015-05-10 10:15:00";
		String dateStr3 = "2015-05-20 09:10:00";
		long t1 = SDF.parse(dateStr1).getTime();
		long t2 = SDF.parse(dateStr2).getTime();
		long t3 = SDF.parse(dateStr3).getTime();
		HTableInterface hTable = utility.createTable(TABLE_NAME, FAMILIES);
		dao = new HBaseDao(hTable);
		dao.updateFirstLogTime(id, action, t1);
		assertEquals(t1, dao.getFirstLogTime(id, action));
		dao.updateFirstLogTime(id, "tnx", t2);
		assertEquals(t2, dao.getFirstLogTime(id, action));
		dao.updateFirstLogTime(id, "tnx", t3);
		assertEquals(t3, dao.getFirstLogTime(id, action));
		dao.close();
	}

	@Test
	public void testUpdateAndGetLastLogTime() throws ParseException, IOException {
		int id = 123456;
		String action = "tnx";
		String dateStr1 = "2015-05-01 10:00:00";
		String dateStr2 = "2015-05-10 10:15:00";
		String dateStr3 = "2015-05-12 09:10:00";
		long t1 = SDF.parse(dateStr1).getTime();
		long t2 = SDF.parse(dateStr2).getTime();
		long t3 = SDF.parse(dateStr3).getTime();
		HTableInterface hTable = utility.createTable(TABLE_NAME, FAMILIES);
		dao = new HBaseDao(hTable);
		dao.updateLastLogTime(id, action, t1);
		assertEquals(-1, dao.getLastLogTime(id, action, SDF.parse("2015-05-01 09:10:00").getTime()));
		dao.updateLastLogTime(id, action, t2);
		assertEquals(t2, dao.getLastLogTime(id, action, SDF.parse("2015-05-12 08:09:00").getTime()));
		boolean rs = dao.updateLastLogTime(id, action, t3);
//		assertEquals(true, rs);
		assertEquals(t2, dao.getLastLogTime(id, action, SDF.parse("2015-05-13 07:00:00").getTime()));
		dao.close();
	}
}
