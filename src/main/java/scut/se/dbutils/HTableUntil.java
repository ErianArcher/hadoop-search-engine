package scut.se.dbutils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HTableUntil {

    public static boolean checkTableExist(String tableName) {
        HBaseOperator op = HBaseOperator.getInstance();
        return op.tableExists(tableName);
    }

    public static List<String> convertStrInCSV2List(String strsInCSV) {
        return Arrays.stream(strsInCSV.split(","))
                .map(String::trim).collect(Collectors.toList());
    }

    public static List<Integer> convertIntInCSV2List(String intsInCSV) {
        return Arrays.stream(intsInCSV.split(",")).map(String::trim)
                .map(Integer::parseInt).collect(Collectors.toList());
    }
}
