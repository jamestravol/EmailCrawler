package jt.upwork.crawler;

import java.util.Comparator;

/**
 * URL comparator that moves forward the urls with mentioned keywords
 *
 * @author jamestravol
 */
public final class UrlComparator implements Comparator<String> {

    private static final String[] KEYWORDS = {"contact", "email", "about"};

    static final UrlComparator INSTANCE = new UrlComparator();

    private UrlComparator() {
    }

    @Override
    public int compare(String o1, String o2) {

        boolean o1contains = containsKeywords(o1);
        boolean o2contains = containsKeywords(o2);

        if (o1contains && o2contains) {
            return 0;
        } else if (o1contains) {
            return -1;
        } else if (o2contains) {
            return 1;
        }

        return 0;
    }

    private boolean containsKeywords(String url) {

        if (url != null) {
            for (String keyword : KEYWORDS) {
                if (url.toLowerCase().contains(keyword)) {
                    return true;
                }
            }
        }

        return false;
    }
}
