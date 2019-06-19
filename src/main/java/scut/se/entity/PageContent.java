package scut.se.entity;

import scut.se.dbutils.HTableUntil;

import java.util.List;
import java.util.Objects;


public class PageContent {
    private List<String> words;
    private String html;

    public PageContent() {
    }

    public PageContent(String html, String wordsInCSV) {
        this.words = HTableUntil.convertStrInCSV2List(wordsInCSV);
        this.html = html;
    }

    public List<String> getWords() {
        return words;
    }

    public void setWords(String words) {
        this.words = HTableUntil.convertStrInCSV2List(words);
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
