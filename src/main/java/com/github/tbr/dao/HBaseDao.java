package com.github.tbr.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDao {
	private HBaseTestingUtility utility;
	private static final int MAX_ROWS = 10000;
	private static final int MAX_VERSIONS = 10000;
	private static final byte[] TABLE_NAME = Bytes.toBytes("test_full_log");
	private static final byte[] FAMILY = Bytes.toBytes("last");
	private static final byte[] QUALIFIER = Bytes.toBytes("t");

	public HBaseDao() {
		this.utility = new HBaseTestingUtility();
	}

	public void test(int val) throws Exception {
		startup();
		doTest(val);
		shutdown();
	}

	private void doTest(int val) {
		HTableInterface table = null;
		try {
			table = utility.createTable(TABLE_NAME, FAMILY, MAX_VERSIONS);
			byte[] rowKey = Bytes.toBytes(123456L);
			System.out.println("putting....");
			List<Put> putList = new ArrayList<Put>(MAX_ROWS);
			for (int i = 1; i <= MAX_ROWS; i = i + 3) {
				Put put = new Put(rowKey, i);
				put.add(FAMILY, QUALIFIER, Bytes.toBytes(i + 2));
				putList.add(put);
			}
			table.put(putList);
			System.out.println("getting...");
			Get get = new Get(rowKey);
			get.setTimeRange(0, val);
			get.setMaxVersions(3);
			// Note: it is not possible to filter values for multiple versions
			// get.setFilter(new SingleColumnValueFilter(FAMILY,
			// QUALIFIER,CompareOp.LESS, Bytes.toBytes(val)));
			Result result = table.get(get);
			List<Cell> cells = result.listCells();
			if (cells != null) {
				for (Cell cell : result.listCells()) {
					int actualVal = Bytes.toInt(CellUtil.cloneValue(cell));
					if (actualVal < val) {
						System.out.println("value=" + actualVal + ",ts=" + cell.getTimestamp());
						break;
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					throw new RuntimeException("Closing table instance failed", e);
				}
			}
		}
	}

	private void startup() throws Exception {
		utility.startMiniCluster();
	}

	private void shutdown() throws Exception {
		utility.shutdownMiniCluster();
	}

	public static void main(String[] args) throws Exception {
		HBaseDao dao = new HBaseDao();
		dao.test(10001);
	}
}