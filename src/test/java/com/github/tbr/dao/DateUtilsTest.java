package com.github.tbr.dao;

import java.text.ParseException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.github.tbr.dao.util.DateUtils;

public class DateUtilsTest {

	@Test
	public void test() throws ParseException {
		String dateStr1 = "2015-05-01 10:00:00";
		String dateStr2 = "2015-05-10 10:15:00";
		String dateStr3 = "2015-05-20 09:10:00";
		long t1 = DateUtils.toMills(dateStr1);
		long twf1 = DateUtils.weekFloorOf(t1);
		long twc1 = DateUtils.weekCeilingOf(t1);

		System.out.println(t1);
		System.out.println(DateUtils.toDateStr(t1));
		System.out.println(twf1);
		System.out.println(DateUtils.toDateStr(twf1));
		System.out.println(twc1);
		System.out.println(DateUtils.toDateStr(twc1));

		long t2 = DateUtils.toMills(dateStr2);
		long twf2 = DateUtils.weekFloorOf(t2);
		long twc2 = DateUtils.weekCeilingOf(t2);
		System.out.println(t2);
		System.out.println(DateUtils.toDateStr(t2));
		System.out.println(twf2);
		System.out.println(DateUtils.toDateStr(twf2));
		System.out.println(twc2);
		System.out.println(DateUtils.toDateStr(twc2));

		long t3 = DateUtils.toMills(dateStr3);
		long twf3 = DateUtils.weekFloorOf(t3);
		long twc3 = DateUtils.weekCeilingOf(t3);
		System.out.println(t3);
		System.out.println(DateUtils.toDateStr(t3));
		System.out.println(twf3);
		System.out.println(DateUtils.toDateStr(twf3));
		System.out.println(twc3);
		System.out.println(DateUtils.toDateStr(twc3));

		
		byte[] b1 = Bytes.toBytes(t2);
		System.out.print(Arrays.toString(b1));
		byte[] b2 = Bytes.toBytes(DateUtils.toMills("2015-05-12 09:10:00"));
		System.out.print(Arrays.toString(b2));
		
		System.out.println(DateUtils.weekFloorOf(DateUtils.toMills("2015-05-10 10:15:00")));
		System.out.println(DateUtils.weekFloorOf(DateUtils.toMills("2015-05-12 09:10:00")));
	}
}
