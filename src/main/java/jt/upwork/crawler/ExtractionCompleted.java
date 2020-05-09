package jt.upwork.crawler;

import java.util.Set;

/**
 * Callback interface function
 *
 * @author jamestravol
 */
public interface ExtractionCompleted {

    /**
     * Invokes when the crawling for a single site completed
     *
     * @param webSite original website
     * @param emails  the result
     */
    void onComplete(WebSite webSite, Set<String> emails);

}
