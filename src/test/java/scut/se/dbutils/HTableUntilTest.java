package scut.se.dbutils;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class HTableUntilTest {

    private HBaseOperator op = null;

    @Before
    public void setup() {
        op = HBaseOperator.getInstance();
    }

    @Test
    public void checkTableExist() {
        String tableName = "testTable";
        op.createTable(tableName, Collections.singletonList("testCol"));
        assertTrue(HTableUntil.checkTableExist(tableName));
        op.deleteTable(tableName);
        assertFalse(HTableUntil.checkTableExist(tableName));
    }
}