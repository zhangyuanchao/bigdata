package com.themis.flink.model;

import java.sql.Timestamp;

public class UrlView {

    public String url;
    public Long count;
    public Long start;
    public Long end;

    public UrlView() {
    }

    public UrlView(String url, Long count, Long start, Long end) {
        this.url = url;
        this.count = count;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return "UrlView{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", start=" + new Timestamp(start) +
                ", end=" + new Timestamp(end) +
                '}';
    }
}
