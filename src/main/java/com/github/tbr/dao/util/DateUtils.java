package com.github.tbr.dao.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {
	private static final long WEEK_MS = 7 * 24 * 3600 * 1000L;
	private static final DateFormat SDF = new SimpleDateFormat("yy-MM-dd HH:mm:ss");

	static {
		System.out.println("default timezone " + SDF.getTimeZone());
		SDF.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	public static long toMills(String dateStr) throws ParseException {
		return SDF.parse(dateStr).getTime();
	}

	public static String toDateStr(long timestamp) {
		return SDF.format(new Date(timestamp));
	}

	public static long weekFloorOf(long timestamp) {
		return timestamp / WEEK_MS * WEEK_MS;
	}

	public static long weekCeilingOf(long timestamp) {
		return (timestamp + WEEK_MS) / WEEK_MS * WEEK_MS;
	}
}