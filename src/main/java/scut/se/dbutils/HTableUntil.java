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
        String strsInCSV_tmp = strsInCSV.replaceAll("\\[", "").replaceAll("]", "");
        return Arrays.stream(strsInCSV_tmp.split(","))
                .map(String::trim).collect(Collectors.toList());
    }

    public static List<Integer> convertIntInCSV2List(String intsInCSV) {
        String intsInCSV_tmp = intsInCSV.replaceAll("\\[", "").replaceAll("]", "");
        return Arrays.stream(intsInCSV_tmp.split(",")).map(String::trim)
                .map(Integer::parseInt).collect(Collectors.toList());
    }
}
