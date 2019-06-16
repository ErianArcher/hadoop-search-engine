import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseOperatorTest {

    HBaseOperator operator = null;
    String tableName = "test";

    @Before
    public void setUp() throws Exception {
        operator = HBaseOperator.getInstance();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void insertOneRowTo() {
    }

    @Test
    public void getResultScanner() {
    }

    @Test
    public void getColValWithKeyword() {
    }

    @Test
    public void getColValWithKeywordInSubStr() {
    }

    @Test
    public void getRowData() {
    }

    @Test
    public void getColumnValue() {
    }
}