package com.hbase.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.hbase.api.domain.ColumnResult;
import com.hbase.api.domain.HData;
import com.hbase.api.impl.HbaseOperatorImpl;


public class HbaseOperaTest {
	HbaseOperatorImpl base = null;

	private void getConf() {
		base = new HbaseOperatorImpl();
	}

	private static String name = "zk_lk";
	private static String row = "123456789545131215487432415";

	@Test
	public void creatTable() throws Exception {
		getConf();
		String[] array = { "col1", "col2"};
		base.creatTable(name, array);
	}

	@Test
	public void addData() throws Exception {
		getConf();

		List<HData> dataList = new ArrayList<HData>();
		HData data = new HData();
		data.setFamilyColumu("col1");
		List<String> nameList = new ArrayList<String>();
		nameList.add("id");
		nameList.add("age");
		data.setNameList(nameList);
		List<Object> valueList = new ArrayList<Object>();
		valueList.add("2");
		valueList.add("24");
		data.setValueList(valueList);
		
		HData data2 = new HData();
		data2.setFamilyColumu("col2");
		List<String> nameList2 = new ArrayList<String>();
		nameList2.add("nan");
		nameList2.add("nv");
		data2.setNameList(nameList2);
		List<Object> valueList2 = new ArrayList<Object>();
		valueList2.add("n1");
		valueList2.add("v1");
		data2.setValueList(valueList2);
		
		dataList.add(data2);
		dataList.add(data);
		
		base.addData(row, name, dataList );
		
	}

	@Test
	public void getResultScannByRowKey() throws Exception {
		getConf();
		base.getResultScann(name);
	}
	
	
	@Test
	public void getFamilyColumn() throws IOException{
		getConf();
		Map<String, String> map = base.getFamilyColumn(name, row, "col1");
		System.out.println(map.size());
		System.out.println(map.get("id"));
		System.out.println(map.get("age"));
	}

	@Test
	public void getColumn() throws Exception {
		getConf();
		ColumnResult a = base.getColumn(name, row, "col1", "id");
		System.out.println("name = " + a.getName() + "   value = " + a.getValue());
	}
	
	@Test
	public void getColumnValue() throws Exception {
		getConf();
		byte[] a = base.getColumnValue(name, row, "col1", "id");
		System.out.println(new String(a));
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
