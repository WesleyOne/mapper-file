package io.github.wesleyone.store.file;

import java.util.StringJoiner;

/**
 * @author http://wesleyone.github.io/
 */
public class DefaultMessage {

    private String title;

    private String body;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DefaultMessage.class.getSimpleName() + "[", "]")
                .add("title='" + title + "'")
                .add("body='" + body + "'")
                .toString();
    }
}
