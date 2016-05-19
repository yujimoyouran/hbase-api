package com.hbase.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;

import com.hbase.api.domain.ColumnResult;
import com.hbase.api.domain.HData;


public interface HbaseOperator {

	/**
	 * 创建表
	 * @param tableName
	 * @param family
	 * @throws Exception
	 */
	void creatTable(String tableName, String[] family)throws Exception;
	
	/**
	 * 判断hbase表是否已经存在
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean existTable(String tableName) throws IOException;
	
	/**
     * 根据rwokey查询
     * @param tableName
     * @param row
     * @throws Exception
     */
    public Result getOneRow(String tableName,String row)throws Exception;
	
	
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
	void addData(String rowKey, String tableName, List<HData> dataList) throws Exception;
	
	void addData(String rowKey, String tableName, String columnFamilyName, String column1,
			byte[] value1) throws Exception;
    
    /**
     * 获取表的一个列族信息
     * 
     * @param tableName
     * @param row
     * @param familyColumn
     * @return 
     * @throws Exception 
     */
    public Map<String, String> getFamilyColumn(String tableName, String row, String familyColumn) throws Exception;
    
	/**
	 *  遍历查询hbase表
     * 
     * @tableName 表名
	 * @throws Exception
	 */
	void getResultScann(String tableName) throws Exception;
	/**
	   * 查看某个表下的所有数据
	   * 
	   * @param tableName 表名
	   * @param rowKey  行健
	   * */
	  public void getResultScannByRowKey(String tableName,String rowKey)throws Exception;
	/**
	 * 遍历查询hbase表
     * 
     * @tableName 表名	
	 * @param tableName
	 * @param start_rowkey
	 * @param stop_rowkey
	 * @throws Exception
	 */
	void getResultScann(String tableName, String start_rowkey,
            String stop_rowkey) throws Exception;
	
	/**
	 * 查询表中的某一列的值
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
	 * @param familyName
	 * @param columnName
	 * @return 
	 * @throws Exception
	 */
	byte[] getColumnValue(String tableName, String rowKey,
            String familyName, String columnName) throws Exception;
	
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
	ColumnResult getColumn(String tableName, String rowKey,
			String familyName, String columnName) throws Exception;
	/**
	 * 更新表中的某一列
	 * @return 
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
	boolean updateTable(String tableName, String rowKey,
            String familyName, String columnName, String value)
            throws Exception;
	/**
	 * 查询某列数据的多个版本
	 * 
	 * @param tableName 表名
	 * @param rowKey 行健
	 * @param familyName 列族
	 * @param columnName	列名
	 * @param versionNum	版本数量
	 * @throws Exception
	 */
	public void getResultByVersion(String tableName, String rowKey,
			String familyName, String columnName, int versionNum) throws Exception;
	
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
	void getResultByVersion(String tableName, String rowKey,
            String familyName, String columnName) throws Exception;
	
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
	void deleteColumn(String tableName, String rowKey,
            String falilyName, String columnName) throws Exception;
	
	/**
	 * 删除指定的列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
	 * @throws Exception
	 */
	void deleteAllColumn(String tableName, String rowKey)
            throws Exception;
	/**
	 * 删除表
     * 
     * @tableName 表名
	 * @throws IOException
	 */
	void deleteTable(String tableName) throws Exception;
	/**
	   * 删除多条数据
	   * @param tableName 表名
	   * @param rows 行健集合
	   * 
	   * **/
	  public void deleteList(String tableName,String rows[])throws Exception;
	
	
	
	
	
	
	
	
	
	
	
	
	
}
