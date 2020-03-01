package com.marklogic.client;

import org.apache.kafka.common.header.Header;

public class UriHeader implements Header {
    String key;
    String value;

    UriHeader(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value.getBytes();
    }

    public String getValue() { return value; }
}
