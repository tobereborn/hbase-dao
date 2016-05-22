package com.github.tbr.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.github.tbr.dao.util.DateUtils;

public class HBaseDao {
	private HTableInterface hTable;

	private static final byte[] FIRST_LOG_TIME_FAMILY = Bytes.toBytes("f");
	private static final byte[] LAST_LOG_TIME_FAMILY = Bytes.toBytes("l");

	public HBaseDao(HTableInterface hTable) {
		this.hTable = hTable;
	}

	public boolean updateFirstLogTime(int id, String action, long logTime) throws IOException {
		byte[] rowKey = rowKey(id, 0L);
		byte[] family = FIRST_LOG_TIME_FAMILY;
		byte[] qualifier = Bytes.toBytes(action);
		byte[] value = Bytes.toBytes(logTime);
		return update(rowKey, family, qualifier, CompareOp.GREATER, value);
	}

	public boolean updateLastLogTime(int id, String action, long logTime) throws IOException {
		long week = DateUtils.weekFloorOf(logTime);
		byte[] rowKey = rowKey(id, week);
		byte[] family = LAST_LOG_TIME_FAMILY;
		byte[] qualifier = Bytes.toBytes(action);
		byte[] value = Bytes.toBytes(logTime);
		return update(rowKey, family, qualifier, CompareOp.LESS, value);
	}

	private boolean update(byte[] rowKey, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value)
			throws IOException {
		try {
			Put put = new Put(rowKey);
			if (!hTable.checkAndPut(rowKey, family, qualifier, value, put)) {
				RowMutations rowMutations = new RowMutations(rowKey);
				rowMutations.add(put);
				return hTable.checkAndMutate(rowKey, family, qualifier, compareOp, value, rowMutations);
			}
			return false;
		} finally {
			if (hTable != null) {
				hTable.close();
			}
		}
	}

	private byte[] rowKey(int id, long week) {
		return Bytes.add(Bytes.toBytes(id), Bytes.toBytes(week));
	}

	public long getFirstLogTime(int id, String action) throws IOException {
		try {
			Get get = new Get(rowKey(id, 0L));
			List<Cell> cells = hTable.get(get).listCells();
			return cells == null ? -1 : Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
		} finally {
			if (hTable != null) {
				hTable.close();
			}
		}
	}

	public long getLastLogTime(int id, String action, long currentLogTime) throws IOException {
		byte[] startRow = rowKey(id, 0);
		byte[] stopRow = rowKey(id, DateUtils.weekCeilingOf(currentLogTime));
		Scan scan = new Scan();
		scan.setStartRow(startRow);
		scan.setStopRow(stopRow);
		scan.setReversed(false);
		scan.addColumn(FIRST_LOG_TIME_FAMILY, Bytes.toBytes(action));
		ResultScanner scanner = null;
		try {
			scanner = hTable.getScanner(scan);
			for (Result rs : scanner) {
				for (Cell cell : rs.listCells()) {
					long lastLogTime = Bytes.toLong(CellUtil.cloneValue(cell));
					if (lastLogTime < currentLogTime) {
						return lastLogTime;
					}
				}
			}
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			if (hTable != null) {
				hTable.close();
			}
		}

		return -1;
	}
}