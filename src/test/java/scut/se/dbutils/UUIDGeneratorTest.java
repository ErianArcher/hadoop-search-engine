package scut.se.dbutils;

import org.junit.Test;

import static org.junit.Assert.*;

public class UUIDGeneratorTest {

    @Test
    public void getUUIDName() {
        String newUUID = UUIDGenerator.getUUID();
        assertEquals(32, newUUID.length());
    }
}