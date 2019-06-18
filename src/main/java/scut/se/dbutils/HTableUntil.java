package scut.se.dbutils;

public class HTableUntil {

    public static boolean checkTableExist(String tableName) {
        HBaseOperator op = HBaseOperator.getInstance();
        return op.tableExists(tableName);
    }
}
