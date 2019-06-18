package scut.se.dbutils;

import java.text.MessageFormat;
import java.util.Objects;
import java.util.UUID;

public class RowKeyGenerator {
    public static String getUUID(){
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static String getHash(String str4RowKey) {
        return MessageFormat.format("{0}{1}", String.valueOf(Objects.hash(str4RowKey)), str4RowKey);
    }
}
