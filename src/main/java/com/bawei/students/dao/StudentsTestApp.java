/**
 * Create Date:2019年9月24日
 */
package com.bawei.students.dao;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

/**
 * <br>Title:TODO 类标题
 * <br>Description:TODO 类功能描述
 * <br>Author:原瑞强(467599360@qq.com)
 * <br>Date:2019年9月24日
 */
public class StudentsTestApp {
  public static void main(String[] args) throws IOException {
    StudentsDao studentsDao = new StudentsDao();
    /*    studentsDao.initData();
    studentsDao.addRowDataBath(readData());
    */
    Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("base-info"), Bytes.toBytes("country"),
      CompareOp.EQUAL, new SubstringComparator("USA"));
    Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("base-info"), Bytes.toBytes("country"),
      CompareOp.EQUAL, new SubstringComparator("China"));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE, filter1, filter2);
    System.out.println(studentsDao.findStudentsInfo(filterList));
  }

  public static Map<String, Map<String, String>> readData() throws JsonProcessingException, IOException {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode nodes = mapper
      .readTree(StudentsTestApp.class.getClassLoader().getSystemResourceAsStream("students.json"));

    Map<String, Map<String, String>> result = new LinkedHashMap<String, Map<String, String>>();
    for (JsonNode node : nodes) {
      Map<String, String> columnMap = new LinkedHashMap<>();
      String rowKey = "";

      if (node.get("name") != null) {
        columnMap.put("base-info:name", node.get("name").toString().replace("\"", ""));
        rowKey = node.get("name").toString().replace("\"", "");
      }
      if (node.get("age") != null) {
        columnMap.put("base-info:age", node.get("age").toString());
      }
      if (node.get("email") != null) {
        columnMap.put("base-info:email", node.get("email").toString().replace("\"", ""));
      }
      if (node.get("country") != null) {
        columnMap.put("base-info:country", node.get("country").toString().replace("\"", ""));
      }

      if (node.get("c") != null) {
        columnMap.put("scoures:c", node.get("c").toString());
      }
      if (node.get("e") != null) {
        columnMap.put("scoures:e", node.get("e").toString());
      }
      if (node.get("m") != null) {
        columnMap.put("scoures:m", node.get("m").toString());
      }

      JsonNode books = node.get("books");
      if (books != null) {
        StringBuffer buffer = new StringBuffer();
        for (JsonNode book : books) {
          buffer.append(book.toString().replace("\"", ""));
          buffer.append(",");
        }
        buffer.deleteCharAt(buffer.lastIndexOf(","));
      }

      JsonNode address = node.get("address");
      if (address != null) {
        if (address.get("city") != null) {
          columnMap.put("address:city", address.get("city").toString().replace("\"", ""));
        }
        if (address.get("street") != null) {
          columnMap.put("address:street", address.get("street").toString().replace("\"", ""));
        }
        if (address.get("province") != null) {
          columnMap.put("address:province", address.get("province").toString().replace("\"", ""));
        }
      }

      result.put(rowKey + "_" + System.currentTimeMillis() + "_" + (int) (Math.random() * 1000), columnMap);
    }
    return result;

  }
}
