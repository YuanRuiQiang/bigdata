/**
 * Create Date:2019年9月24日
 */
package com.bawei.students.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <br>Title:TODO 类标题
 * <br>Description:TODO 类功能描述
 * <br>Author:原瑞强(467599360@qq.com)
 * <br>Date:2019年9月24日
 */
public class StudentsDao {
  private static Configuration configuration;

  private static Admin admin;

  public static String NAMESPACE = "stumanager";

  public static String TABLENAME = "stuinfo";

  public static String BASEINFO = "base-info";

  public static String SCOURES = "scoures";

  public static String BOOKS = "books";

  public static String ADDRESS = "address";

  static {
    configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "192.168.100.88");
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
  }

  public boolean initData() throws IOException {
    boolean result = true;
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(NAMESPACE).build();
    admin = getConnection().getAdmin();
    admin.createNamespace(namespaceDescriptor);

    if (admin.tableExists(TableName.valueOf(TABLENAME))) {
      System.out.println("表已存在");
      return false;
    }
    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLENAME));
    HColumnDescriptor CF_1 = new HColumnDescriptor(BASEINFO);
    HColumnDescriptor CF_2 = new HColumnDescriptor(SCOURES);
    HColumnDescriptor CF_3 = new HColumnDescriptor(BOOKS);
    HColumnDescriptor CF_4 = new HColumnDescriptor(ADDRESS);
    hTableDescriptor.addFamily(CF_1);
    hTableDescriptor.addFamily(CF_2);
    hTableDescriptor.addFamily(CF_3);
    hTableDescriptor.addFamily(CF_4);
    admin.createTable(hTableDescriptor);
    return result;

  }

  public void addRowDataBath(Map<String, Map<String, String>> data) throws IOException {

    Table table = getConnection().getTable(TableName.valueOf(TABLENAME));

    List<Put> puts = new ArrayList<>();

    Set<Entry<String, Map<String, String>>> entrySet = data.entrySet();
    for (Entry<String, Map<String, String>> entry : entrySet) {
      Put put = new Put(Bytes.toBytes(entry.getKey()));
      Set<Entry<String, String>> columns = entry.getValue().entrySet();
      for (Entry<String, String> column : columns) {
        put.addColumn(Bytes.toBytes(column.getKey().split(":")[0]),
          Bytes.toBytes(column.getKey().split(":").length > 1 ? column.getKey().split(":")[1] : ""),
          Bytes.toBytes(column.getValue()));
      }
      puts.add(put);
    }
    table.put(puts);
    table.close();
  }

  public Map<String, Map<String, String>> findStudentsInfo(Filter filter) throws IOException {
    Table table = getConnection().getTable(TableName.valueOf(TABLENAME));
    Map<String, Map<String, String>> result = new LinkedHashMap<String, Map<String, String>>();
    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    for (Result temp : scanner) {
      Map<String, String> columnMap = new LinkedHashMap<>();
      String stuName = "";
      List<Cell> cells = temp.listCells();
      for (Cell cell : cells) {
        if ("".equals(stuName)) {
          stuName = Bytes.toString(CellUtil.cloneRow(cell));
        }
        columnMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
          Bytes.toString(CellUtil.cloneValue(cell)));
      }
      result.put(stuName, columnMap);
    }
    return result;

  }

  public static Connection getConnection() throws IOException {
    return ConnectionFactory.createConnection(configuration);
  }
}
