package com.hbase.api;

import org.junit.Test;

import com.hbase.api.impl.DBOperatorByHBase;

public class FileOperatorByHBase {
	DBOperatorByHBase base = null;

	private void getConf() {
		if (base == null)
			base = new DBOperatorByHBase(
					"Thadoop5:2181,Thadoop6:2181,Thadoop7:2181");
	}

	private static String name = "zk_lk";
	private static String row = "123456789545131215487432415";

	@Test
	public void creatTable() throws Exception {
		getConf();
		String[] array = { "col1", "col2", "col3" };
		base.creatTable(name, array);
	}

	@Test
	public void addData() throws Exception {
		getConf();
		String[] column1 = {"id", "age"};
		String[] value1 = {"1", "23"};
		String[] column2 = {"sex", "bizi"};
		String[] value2 = {"nan", "2ge"};
		String[] column3 = {"zuiba", "yanjing"};
		String[] value3 = {"dazuiba", "dayanjing"};
		base.addData(row, name, column1, value1, column2, value2, column3, value3);
		
	}

	@Test
	public void getOneRow() throws Exception {
		getConf();
		base.getOneRow(name, row);
	}

	@Test
	public void getResultScannByRowKey() throws Exception {
		getConf();
		base.getResultScann(name);
	}

	@Test
	public void getResultByColumn() throws Exception {
		getConf();
		base.getResultByColumn(name, row, "col1", "id");
	}

	@Test
	public void updateTable() throws Exception {
		getConf();
		base.updateTable(name, row, "col1", "id", "2");
	}

	@Test
	public void getResultByVersion() throws Exception {
		getConf();
		base.getResultByVersion(name, row, "col1", "id");
	}

	@Test
	public void deleteAllColumn() throws Exception {
		getConf();
//		base.deleteColumn(name, row, "col1", "age");
		base.deleteAllColumn(name, row);
	}


	@Test
	public void deleteTable() throws Exception {
		getConf();
		base.deleteTable(name);
	}

}
