package scut.se.entity;

import scut.se.dbutils.HTableUntil;

import java.util.List;
import java.util.Objects;

public class InvertedIndex {
    private String word;
    private List<String> htmlNOs;
    private List<Integer> counts;

    public InvertedIndex() {
    }

    public InvertedIndex(String word, String htmlNOsInCSV, String countsInCSV) {
        this.word = word;
        this.htmlNOs = HTableUntil.convertStrInCSV2List(htmlNOsInCSV);
        this.counts = HTableUntil.convertIntInCSV2List(countsInCSV);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public List<String> getHtmlNOs() {
        return htmlNOs;
    }

    public void setHtmlNOs(String htmlNOs) {
        this.htmlNOs = HTableUntil.convertStrInCSV2List(htmlNOs);
    }

    public List<Integer> getCounts() {
        return counts;
    }

    public void setCounts(String counts) {
        this.counts = HTableUntil.convertIntInCSV2List(counts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvertedIndex that = (InvertedIndex) o;
        return word.equals(that.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word);
    }
}
