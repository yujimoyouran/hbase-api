package com.hbase.api.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

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
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hbase.api.HbaseOperator;
import com.hbase.api.constant.HbaseConstant;
import com.hbase.api.domain.ColumnResult;
import com.hbase.api.domain.HData;

public class HbaseOperatorImpl implements HbaseOperator {

	private static Logger logger = Logger.getLogger(HbaseOperatorImpl.class);
	// 声明静态配置
	private static Configuration conf = null;
	private static HBaseAdmin admin = null;
	private static HConnection conn = null;
	private static String QUORUM = HbaseConstant.QUORUM;

	/**
	 * 获取全局唯一的Configuration实例
	 * 
	 * @return
	 */
	public static synchronized Configuration getConfiguration() {
		if (conf == null) {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", QUORUM);
		}
		return conf;
	}

	/**
	 * 获取全局唯一的HConnection实例
	 * 
	 * @return
	 * @throws IOException 
	 */
	public static synchronized HConnection getHConnection()
			throws IOException {
		if (conn == null) {
			conn = HConnectionManager.createConnection(getConfiguration());
		}
		return conn;
	}


	public HbaseOperatorImpl() {
	}

	private void getHbaseAdmin()
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		if(conf == null){
			HbaseOperatorImpl.getConfiguration();
		}
		if (admin == null) {
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
		getHbaseAdmin();
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
	 * 判断hbase表是否已经存在
	 * 
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean existTable(String tableName) throws IOException {
		getHbaseAdmin();
		return admin.tableExists(tableName);
	}

	/**
	 * 为表添加数据（适合知道有多少列族的固定表）
	 * 
	 * @rowKey rowKey
	 * 
	 * @tableName 表名
	 * 
	 * @columnFamilyName 列族
	 * 
	 * @column1 第一个列族列表
	 * 
	 * @value1 第一个列的值的列表
	 * 
	 */
	public void addData(String rowKey, String tableName, List<HData> dataList)
			throws Exception {
		Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
//		table = new HTable(conf, Bytes.toBytes(tableName));
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
		// 获取表
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
				.getColumnFamilies();

		for (int i = 0; i < columnFamilies.length; i++) {
			String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
			for (int j = 0; j < dataList.size(); j++) {
				if (familyName.equals(dataList.get(j).getFamilyColumu())) {
					for (int n = 0; n < dataList.get(j).getNameList().size(); n++) {
						put.add(Bytes
								.toBytes(dataList.get(j).getFamilyColumu()),
								Bytes.toBytes(dataList.get(j).getNameList()
										.get(n)), Bytes.toBytes(dataList.get(j)
										.getValueList().get(n)
										+ ""));
					}
				}
			}
		}
		table.put(put);
		System.out.println("add data Success!");
		table.close();
	}

	public void addData(String rowKey, String tableName,
			String columnFamilyName, String column1, byte[] value1)
			throws Exception {
		Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
		// 获取表
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
				.getColumnFamilies();

		for (int i = 0; i < columnFamilies.length; i++) {
			String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
			if (familyName.equals(columnFamilyName)) { // article列族put数据
				put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1),
						value1);
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
	public Result getOneRow(String tableName, String row) throws Exception {
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		table.close();// 释放资源
		return result;
	}

	/**
	 * 获取表的一个列族信息
	 * 
	 * @param tableName
	 * @param row
	 * @param familyColumn
	 * @return
	 * @throws IOException
	 */
	public Map<String, String> getFamilyColumn(String tableName, String rowKey,
			String family) throws IOException {
		// 返回结果
		Map<String, String> resultMap = new HashMap<String, String>();

		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setMaxVersions();

		Result result = table.get(get);

		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result
				.getMap();

		if (map == null) {
			return resultMap;
		}

		// 取得指定family下的数据
		NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map
				.get(Bytes.toBytes(family));
		if (familyMap == null) {
			return resultMap;
		}

		Set<byte[]> familyMapSet = familyMap.keySet();

		for (Iterator<byte[]> familyIt = familyMapSet.iterator(); familyIt
				.hasNext();) {
			byte[] keyColumn = (byte[]) familyIt.next();
			NavigableMap<Long, byte[]> columnMap = familyMap.get(keyColumn);

			Set<Long> columnMapSet = columnMap.keySet();
			for (Iterator<Long> columnIt = columnMapSet.iterator(); columnIt
					.hasNext();) {
				Long columnKey = (Long) columnIt.next();
				String columnValue = new String(columnMap.get(columnKey));
				resultMap.put(new String(keyColumn), columnValue);
			}
		}
		return resultMap;
	}

	/**
	 * 查询表中的某一列值
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
	public byte[] getColumnValue(String tableName, String rowKey,
			String familyName, String columnName) throws Exception {
		Result result = getResult(tableName, rowKey, familyName, columnName);
		return result.getValue(Bytes.toBytes(familyName),
				Bytes.toBytes(columnName));
	}

	/**
	 * 查询表中的某一列
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey
	 * @param familyName
	 * @param columnName
	 * @throws Exception
	 */
	public ColumnResult getColumn(String tableName, String rowKey,
			String familyName, String columnName) throws Exception {
		Result result = getResult(tableName, rowKey, familyName, columnName);
		return getColumnResult(result);
	}

	private ColumnResult getColumnResult(Result result) throws Exception {
		ColumnResult columnResult = new ColumnResult();
		for (Cell cell : result.rawCells()) {
			columnResult.setFamilyName(new String(CellUtil.cloneFamily(cell)));
			columnResult.setTimestamp(cell.getTimestamp());
			columnResult.setName(new String(CellUtil.cloneQualifier(cell)));
			columnResult.setValue(new String(CellUtil.cloneValue(cell)));
		}
		return columnResult;

	}

	private Result getResult(String tableName, String rowKey,
			String familyName, String columnName) throws Exception {
		HTableInterface table =  null;
		try {
			table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
			return table.get(get);
		} finally{
			table.close();
		}
		
	}

	// private List<HResult> getHResult(Result result) {
	// List<HResult> list = new ArrayList<HResult>();
	//
	// for (Cell cell : result.rawCells()) {
	// Map<String, byte[]> map = new HashMap<String, byte[]>();
	// HResult hResult = new HResult();
	// hResult.setRowKey(new String(CellUtil.cloneRow(cell)));
	// hResult.setFamilyColumn(new String(CellUtil.cloneFamily(cell)));
	// map.put(new String(CellUtil.cloneQualifier(cell)),
	// CellUtil.cloneValue(cell));
	// hResult.setMap(map);
	// hResult.setTimestamp(cell.getTimestamp());
	//
	// list.add(hResult);
	// }
	//
	// return list;
	// }

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
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
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
	public boolean updateTable(String tableName, String rowKey, String familyName,
			String columnName, String value) throws Exception {
		HTableInterface table = null;
		try {
			table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("update table Success!");
			return true;
		}finally{
			table.close();
			conn.close();
		}
		
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
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
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
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
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
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
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
		HTableInterface table = HbaseOperatorImpl.getHConnection().getTable(Bytes.toBytes(tableName));
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
		getHbaseAdmin();
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		System.out.println(tableName + "is deleted!");
	}

	/**
	 * 根据表名,rowKey,family及时间戳[startTs,endTs)查询相关数据.
	 * 
	 */
	public Map<String, Map<Long, String>> queryMultiVersions(String tableName,
			String rowKey, String family) {
		// 返回结果
		Map<String, Map<Long, String>> resultMap = new HashMap<String, Map<Long, String>>();

		HTable hTable = null;

		try {
			hTable = new HTable(conf, tableName);

			Get get = new Get(Bytes.toBytes(rowKey));
			get.setMaxVersions();

			Result result = hTable.get(get);

			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result
					.getMap();

			if (map == null) {
				return resultMap;
			}

			// 取得指定family下的数据
			NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map
					.get(Bytes.toBytes(family));
			if (familyMap == null) {
				return resultMap;
			}

			Set<byte[]> familyMapSet = familyMap.keySet();

			for (Iterator<byte[]> familyIt = familyMapSet.iterator(); familyIt
					.hasNext();) {
				byte[] keyColumn = (byte[]) familyIt.next();
				NavigableMap<Long, byte[]> columnMap = familyMap.get(keyColumn);

				Map<Long, String> mapColumn = new HashMap<Long, String>();

				Set<Long> columnMapSet = columnMap.keySet();
				for (Iterator<Long> columnIt = columnMapSet.iterator(); columnIt
						.hasNext();) {
					Long columnKey = (Long) columnIt.next();
					String columnValue = new String(columnMap.get(columnKey));
					mapColumn.put(columnKey, columnValue);
				}

				resultMap.put(new String(keyColumn), mapColumn);
			}

		} catch (Exception e) {
			logger.error("查询HBASE出错,rowKey[" + rowKey + "]", e);

		} finally {
			if (hTable != null) {
				// hTablePool.putTable(hTable);
			}
		}
		return resultMap;
	}
}
