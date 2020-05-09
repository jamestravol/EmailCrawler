package jt.upwork.crawler;

import java.net.URL;

/**
 * A site representation
 *
 * @author jamestravol
 */
public class WebSite {

    private final String id;

    private final URL url;

    public WebSite(String id, URL url) {
        this.id = id;
        this.url = url;
    }

    public String getId() {
        return id;
    }

    public URL getUrl() {
        return url;
    }

    @Override
    public String toString() {
        return "WebSite{" +
                "id='" + id + '\'' +
                ", url=" + url +
                '}';
    }
}
