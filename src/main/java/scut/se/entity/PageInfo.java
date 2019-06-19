package scut.se.entity;

import java.util.Objects;

public class PageInfo {
    private String url;
    private String title;
    private String uri;

    public PageInfo() {
    }

    public PageInfo(String url, String title, String uri) {
        this.url = url;
        this.title = title;
        this.uri = uri;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageInfo pageInfo = (PageInfo) o;
        return url.equals(pageInfo.url) &&
                title.equals(pageInfo.title) &&
                uri.equals(pageInfo.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, title, uri);
    }
}
