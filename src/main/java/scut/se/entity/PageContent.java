package scut.se.entity;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PageContent {
    private List<String> words;
    private String html;

    public PageContent(String html, String wordsInCSV) {
        this.words = Arrays.stream(wordsInCSV.split(","))
                .map(String::trim).collect(Collectors.toList());
        this.html = html;
    }

    public List<String> getWords() {
        return words;
    }

    public void setWords(List<String> words) {
        this.words = words;
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageContent that = (PageContent) o;
        return html.equals(that.html);
    }

    @Override
    public int hashCode() {
        return Objects.hash(html);
    }
}
