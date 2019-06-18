package scut.se.dbutils;

import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.*;

public class RowKeyGeneratorTest {

    @Test
    public void getUUIDName() {
        String newUUID = RowKeyGenerator.getUUID();
        assertEquals(32, newUUID.length());
    }

    @Test
    public void getHash() {
        String word = "hello";
        String expected = String.valueOf(Objects.hash(word)) + word;
        assertEquals(expected, RowKeyGenerator.getHash(word));
    }
}