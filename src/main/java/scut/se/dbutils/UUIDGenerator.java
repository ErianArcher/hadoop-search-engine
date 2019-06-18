package scut.se.dbutils;

import java.util.UUID;

public class UUIDGenerator {
    public static String getUUID(){
        String uuid = UUID.randomUUID().toString().replace("-", "");
        return uuid;
    }
}
