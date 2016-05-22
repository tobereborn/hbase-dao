package com.github.tbr.dao.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class DateUtils {
	private static final long WEEK_MS = 7 * 3600 * 1000L;
	private static final DateFormat SDF = new SimpleDateFormat("");

	static {
		SDF.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	public static long toMills(String dateStr) throws ParseException {
		return SDF.parse(dateStr).getTime();
	}

	public static long weekFloorOf(long timestamp) {
		return timestamp % WEEK_MS;
	}

	public static long weekCeilingOf(long timestamp) {
		return (timestamp + WEEK_MS) % WEEK_MS;
	}
}