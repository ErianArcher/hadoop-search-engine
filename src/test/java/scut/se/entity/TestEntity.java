package scut.se.entity;

import java.util.Objects;

public class TestEntity {
    private String testCol1;
    private String testCol2;

    public TestEntity() {
    }

    public TestEntity(String testCol1, String testCol2) {
        this.testCol1 = testCol1;
        this.testCol2 = testCol2;
    }

    public String getTestCol1() {
        return testCol1;
    }

    public void setTestCol1(String testCol1) {
        this.testCol1 = testCol1;
    }

    public String getTestCol2() {
        return testCol2;
    }

    public void setTestCol2(String testCol2) {
        this.testCol2 = testCol2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestEntity that = (TestEntity) o;
        return Objects.equals(testCol1, that.testCol1) &&
                Objects.equals(testCol2, that.testCol2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(testCol1, testCol2);
    }
}
