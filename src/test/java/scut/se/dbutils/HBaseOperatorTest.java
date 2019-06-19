package scut.se.dbutils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scut.se.dbutils.HBaseOperator;
import scut.se.entity.TestEntity;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class HBaseOperatorTest {

    HBaseOperator operator = null;
    String tableName = "test1";
    TestEntity testEntity1 = new TestEntity("value1", "value2");
    TestEntity testEntity2 = new TestEntity("value3, value4", "value5");
    String rowKey1 = "testRowKey1";
    String rowKey2 = "testRowKey2";
    String[] rowKeys = {rowKey1, rowKey2};
    String columnFamily = null;
    List<String> columns = null;
    String updateMsg = "value6";
    String expect1 = MessageFormat.format("{0}->({1}->{2},{3}->{4})", rowKey1,
            "testCol1", testEntity1.getTestCol1(), "testCol2", testEntity1.getTestCol2());
    String expect2 = MessageFormat.format("{0}->({1}->{2},{3}->{4})", rowKey1,
            "testCol1", updateMsg, "testCol2", testEntity1.getTestCol2());
    String expect3 = MessageFormat.format("({0}->({1}->{2},{3}->{4}),{5}->({6}->{7},{8}->{9}))",
            rowKey1, "testCol1", testEntity1.getTestCol1(), "testCol2", testEntity1.getTestCol2(),
            rowKey2, "testCol1", testEntity2.getTestCol1(), "testCol2", testEntity2.getTestCol2());

    private String getResult(Map<String, Map<String, String>> mapping) {
        Map<String, String> m1 = mapping.get(rowKey1);
        Map<String, String> m2 = mapping.get(rowKey2);
        String row1 = MessageFormat.format("{0}->({1}->{2},{3}->{4})", rowKey1, "testCol1", m1.get("testCol1"),
                "testCol2", m1.get("testCol2"));
        // String row2 = MessageFormat.format("{0}->({1}->{2},{3}->{4})", rowKey2, "testCol1", m2.get("testCol1"),
       //         "testCol2", m2.get("testCol2"));
        return MessageFormat.format("({0})", row1);
    }


    @Before
    public void setUp() throws Exception {
        operator = HBaseOperator.getInstance();
        // Get column families
        Class cls = TestEntity.class;
        columnFamily = cls.getName();
        columns = Arrays.stream(cls.getDeclaredFields()).map(Field::getName).collect(Collectors.toList());
        operator.createTable(tableName, Collections.singletonList(columnFamily));
    }

    @After
    public void tearDown() throws Exception {
        operator.deleteTable(tableName);
    }

    @Test
    public void insertUpdateTest() {
        // Assertion for single row
        operator.insertOneRowTo(tableName, testEntity1, rowKey1);
        Map<String, Map<String, String>> result1 = operator.getResultScanner(tableName);
        System.out.println(getResult(result1));
        // Assertion for updated single row
        operator.setColumnValue(tableName, rowKey1, columnFamily, columns.get(0), updateMsg);
        Map<String, Map<String, String>> result2 = operator.getResultScanner(tableName);
        System.out.println(getResult(result2));
        // Assertion for multiple rows
        operator.insertOneRowTo(tableName, testEntity2, rowKey2);
        Map<String, Map<String, String>> result3 = operator.getResultScanner(tableName);
        System.out.println(getResult(result3));
    }

    @Test
    public void getColValWithKeyword() {
        operator.insertOneRowTo(tableName, testEntity1, rowKey1);
        operator.insertOneRowTo(tableName, testEntity2, rowKey2);
        Map<String, Map<String, String>> result = operator.getColValWithKeyword(tableName, columnFamily,
                columns.get(0), "value1");
        // Assertion
        System.out.println(getResult(result));
    }

    @Test
    public void getColValWithKeywordInSubStr() {
        operator.insertOneRowTo(tableName, testEntity1, rowKey1);
        operator.insertOneRowTo(tableName, testEntity2, rowKey2);
        Map<String, Map<String, String>> result = operator.getColValWithKeywordInSubStr(tableName, columnFamily,
                columns.get(0), "value3");
        // Assertion
        Map<String, String> m2 = result.get(rowKey2);
        System.out.println(MessageFormat.format("{0}->({1}->{2},{3}->{4})", rowKey2, "testCol1", m2.get("testCol1"),
                             "testCol2", m2.get("testCol2")));
    }

    @Test
    public void getRowData() {
        operator.insertOneRowTo(tableName, testEntity1, rowKey1);
        operator.insertOneRowTo(tableName, testEntity2, rowKey2);
        Map<String, String> result = operator.getRowData(tableName, rowKey1);
        // Assertion
        for (Map.Entry<String, String> entry: result.entrySet()){
            System.out.println(entry.getKey() + "->" + entry.getValue());
        }
    }

    @Test
    public void getColumnValue() {
        operator.insertOneRowTo(tableName, testEntity1, rowKey1);
        operator.insertOneRowTo(tableName, testEntity2, rowKey2);
        String result = operator.getColumnValue(tableName, rowKey1, columnFamily, columns.get(0));
        assertEquals(testEntity1.getTestCol1(), result);
    }

    @Test
    public void getColumnFamilyPOJOByRowKey() {
    }
}