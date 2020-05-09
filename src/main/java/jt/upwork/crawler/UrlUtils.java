package jt.upwork.crawler;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Some url utilities
 *
 * @author jamestravol
 */
public final class UrlUtils {

    private static final Logger LOGGER = Logger.getLogger(Crawler.class.getName());

    private UrlUtils() {
    }

    public static Optional<URL> makeUrl(URL root, String rest) {

        if (root == null || rest == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(root.toURI().resolve(rest).toURL());
        } catch (MalformedURLException | URISyntaxException e) {
            LOGGER.severe(String.format("Domain '%s' + %s is incorrect", root, rest));
            return Optional.empty();
        }
    }

    public static Optional<URL> makeUrl(String domain) {

        if (domain == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(new URL(domain));
        } catch (MalformedURLException e) {
            LOGGER.severe(String.format("Domain '%s' is incorrect", domain));
            return Optional.empty();
        }
    }

}
