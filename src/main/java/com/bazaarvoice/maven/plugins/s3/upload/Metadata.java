package com.bazaarvoice.maven.plugins.s3.upload;

import java.io.Serializable;

import org.apache.maven.plugins.annotations.Parameter;

public class Metadata implements Serializable {

    private static final long serialVersionUID = -8143541798166470995L;

    private String key;

    private String value;

    /**
     * Applies the Metadata only if file name matches this value (RegExp). It is
     * tested against the file key on aws.
     */
    @Parameter
    private String matches;

    public Metadata(String key, String value, String matches) {
        this.key = key;
        this.value = value;
        this.matches = matches;
    }

    public Metadata() {
        // TODO Auto-generated constructor stub
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMatches() {
        return matches;
    }

    public void setMatches(String matches) {
        this.matches = matches;
    }

    public boolean shouldSetMetadata(String file) {
        if (matches != null && !matches.trim().isEmpty()) {
            if (file.matches(matches)) {
                return true;
            }
            return false;
        }
        return true;
    }

}
