package com.hbase.api.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hbase.api.DBOperator;

public class DBOperatorByHBase implements DBOperator {

	private static Logger logger = Logger.getLogger(DBOperatorByHBase.class);
	// 声明静态配置
	private Configuration conf = null;
	private HBaseAdmin admin = null;
	private HTable table;

	public DBOperatorByHBase(String quorum) {
		if (conf == null) {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", quorum);
		}
	}

	public DBOperatorByHBase(Configuration conf) {
		this.conf = conf;
	}
	
	private void getHbaseAdmin(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		if(admin == null){
			admin = new HBaseAdmin(conf);
		}
	}

	/**
	 * 创建表
	 * 
	 * @tableName 表名
	 * 
	 * @family 列族列表
	 */
	public void creatTable(String tableName, String[] family) throws Exception {
		getHbaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(
				TableName.valueOf(tableName));
		for (int i = 0; i < family.length; i++) {
			desc.addFamily(new HColumnDescriptor(family[i]));
		}
		if (admin.tableExists(tableName)) {
			System.out.println("table Exists!");
		} else {
			admin.createTable(desc);
			System.out.println("create table Success!");
		}
	}

	/**
	 * 为表添加数据（适合知道有多少列族的固定表）
	 * 
	 * @rowKey rowKey
	 * 
	 * @tableName 表名
	 * 
	 * @column1 第一个列族列表
	 * 
	 * @value1 第一个列的值的列表
	 * 
	 * @column2 第二个列族列表
	 * 
	 * @value2 第二个列的值的列表
	 */
	public void addData(String rowKey, String tableName, String[] column1,
			String[] value1, String[] column2, String[] value2,
			String[] column3, String[] value3) throws Exception {
		Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
		table = new HTable(conf, Bytes.toBytes(tableName));
		// 获取表
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
				.getColumnFamilies();

		for (int i = 0; i < columnFamilies.length; i++) {
			String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
			if (familyName.equals("col1")) { // article列族put数据
				for (int j = 0; j < column1.length; j++) {
					put.add(Bytes.toBytes(familyName),
							Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
				}
			} else if (familyName.equals("col2")) { // author列族put数据
				for (int j = 0; j < column2.length; j++) {
					put.add(Bytes.toBytes(familyName),
							Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
				}
			} else if (familyName.equals("col3")) { // author列族put数据
				for (int j = 0; j < column3.length; j++) {
					put.add(Bytes.toBytes(familyName),
							Bytes.toBytes(column3[j]), Bytes.toBytes(value3[j]));
				}
			}
		}
		table.put(put);
		System.out.println("add data Success!");
		table.close();
	}

	/**
	 * 根据rwokey查询
	 * 
	 * @param tableName
	 * @param row
	 * @throws Exception
	 */
	public void getOneRow(String tableName, String row) throws Exception {
		table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		printRecoder(result);// 打印记录
		table.close();// 释放资源
	}

	/**
	 * 打印一条记录的详情
	 * 
	 * */
	private void printRecoder(Result result) throws Exception {
		for (Cell cell : result.rawCells()) {
			System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
			System.out.print(" 列簇: " + new String(CellUtil.cloneFamily(cell)));
			System.out
					.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
			System.out.println(" 时间戳: " + cell.getTimestamp());
			logger.info("行健: " + new String(CellUtil.cloneRow(cell)));
			logger.info(" 列簇: " + new String(CellUtil.cloneFamily(cell)));
			logger.info(" 列: " + new String(CellUtil.cloneQualifier(cell)));
			logger.info(" 值: " + new String(CellUtil.cloneValue(cell)));
			logger.info(" 时间戳: " + cell.getTimestamp());
		}
	}

	/**
	 * 查看某个表下的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            行健
	 * */
	public void getResultScannByRowKey(String tableName, String rowKey)
			throws Exception {
		table = new HTable(conf, tableName);
		Scan scan = new Scan();
		// 指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
		scan.setBatch(1000);
		if (StringUtils.isNotBlank(rowKey))
			scan.setFilter(new PrefixFilter(Bytes.toBytes(rowKey)));
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {
			printRecoder(r);// 打印记录
		}
		table.close();// 释放资源
		rs.close();
	}

	/**
	 * 遍历查询hbase表
	 * 
	 * @param tableName
	 *            表名
	 */
	public void getResultScann(String tableName) throws Exception {
		getResultScannByRowKey(tableName, null);
	}

	/**
	 * 遍历查询hbase表
	 * 
	 * @tableName 表名
	 * @param tableName
	 * @param start_rowkey
	 *            开始的行健
	 * @param stop_rowkey
	 *            结束的行健
	 * @throws Exception
	 */
	public void getResultScann(String tableName, String start_rowkey,
			String stop_rowkey) throws Exception {
		Scan scan = new Scan();
		// 指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
		scan.setBatch(1000);
		scan.setStartRow(Bytes.toBytes(start_rowkey));
		scan.setStopRow(Bytes.toBytes(stop_rowkey));
		ResultScanner rs = null;
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		rs = table.getScanner(scan);
		for (Result r : rs) {
			printRecoder(r);// 打印记录
		}
		table.close();
		rs.close();
	}

	/**
	 * 查询表中的某一列
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey 行健
	 * @param familyName
	 *            列族
	 * @param columnName
	 *            列名称
	 * @throws Exception
	 */
	public void getResultByColumn(String tableName, String rowKey,
			String familyName, String columnName) throws Exception {
		table = new HTable(conf, Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
		Result result = table.get(get);
		printRecoder(result);// 打印记录
		table.close();
	}

	/**
	 * 更新表中的某一列
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey
	 * 
	 * @familyName 列族名
	 * 
	 * @columnName 列名
	 * 
	 * @value 更新后的值
	 * @throws Exception
	 */
	public void updateTable(String tableName, String rowKey, String familyName,
			String columnName, String value) throws Exception {
		table = new HTable(conf, Bytes.toBytes(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
				Bytes.toBytes(value));
		table.put(put);
		System.out.println("update table Success!");
		table.close();
	}

	/**
	 * 查询某列数据的多个版本
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey
	 * 
	 * @familyName 列族名
	 * 
	 * @columnName 列名
	 * @throws Exception
	 */
	public void getResultByVersion(String tableName, String rowKey,
			String familyName, String columnName) throws Exception {
		getResultByVersion(tableName, rowKey, familyName, columnName, 5);
	}

	/**
	 * 查询某列数据的多个版本
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            行健
	 * @param familyName
	 *            列族
	 * @param columnName
	 *            列名
	 * @param versionNum
	 *            版本数量
	 * @throws Exception
	 */
	public void getResultByVersion(String tableName, String rowKey,
			String familyName, String columnName, int versionNum)
			throws Exception {
		table = new HTable(conf, Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		get.setMaxVersions(versionNum);
		Result result = table.get(get);
		printRecoder(result);// 打印记录
		table.close();
	}

	/**
	 * 删除指定的列
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey
	 * 
	 * @familyName 列族名
	 * 
	 * @columnName 列名
	 * @throws IOException
	 */
	public void deleteColumn(String tableName, String rowKey,
			String falilyName, String columnName) throws Exception {
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
		deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
				Bytes.toBytes(columnName));
		table.delete(deleteColumn);
		System.out.println(falilyName + ":" + columnName + "is deleted!");
		table.close();
	}

	/**
	 * 删除指定的列
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey
	 * @throws Exception
	 */
	public void deleteAllColumn(String tableName, String rowKey)
			throws Exception {
		table = new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
		table.delete(deleteAll);
		System.out.println("all columns are deleted!");
		table.close();
	}

	/**
	 * 删除多条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rows
	 *            行健集合
	 * 
	 * **/
	public void deleteList(String tableName, String rows[]) throws Exception {
		table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		for (String row : rows) {
			Delete del = new Delete(Bytes.toBytes(row));
			list.add(del);
		}
		table.delete(list);
		table.close();// 释放资源

	}

	/**
	 * 删除表
	 * 
	 * @tableName 表名
	 * @throws IOException
	 */
	public void deleteTable(String tableName) throws Exception {
		getHbaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		System.out.println(tableName + "is deleted!");
		table.close();
	}

	public void addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2,
			String[] value2) throws Exception {
		// TODO Auto-generated method stub
		
	}


}
