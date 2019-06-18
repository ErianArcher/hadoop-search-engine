package scut.se.entity;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class InvertedIndex {
    private String word;
    private List<String> htmlNOs;
    private List<Integer> counts;

    public InvertedIndex(String word, String htmlNOsInCSV, String countsInCSV) {
        this.word = word;
        this.htmlNOs = Arrays.stream(htmlNOsInCSV.split(",")).map(String::trim).collect(Collectors.toList());
        this.counts = Arrays.stream(countsInCSV.split(",")).map(String::trim)
                .map(Integer::parseInt).collect(Collectors.toList());
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

    public void setHtmlNOs(List<String> htmlNOs) {
        this.htmlNOs = htmlNOs;
    }

    public List<Integer> getCounts() {
        return counts;
    }

    public void setCounts(List<Integer> counts) {
        this.counts = counts;
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
