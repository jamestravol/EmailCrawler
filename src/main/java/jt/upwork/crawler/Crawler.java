package jt.upwork.crawler;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * A simple web crawler for e-mails.
 * Accepts the list of domains.
 * Produces the list of e-mails
 *
 * @author jamestravol
 */
public final class Crawler implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(Crawler.class.getName());

    private final int maxLinksForPage;
    private final int maxInheritance;
    private final ForkJoinPool pool;

    public Crawler(int maxLinksForPage, int maxInheritance, int threadCount) {
        this.maxLinksForPage = maxLinksForPage;
        this.maxInheritance = maxInheritance;
        this.pool = new ForkJoinPool(threadCount);
    }

    public Crawler(int maxLinksForPage, int maxInheritance) {
        this.maxLinksForPage = maxLinksForPage;
        this.maxInheritance = maxInheritance;
        this.pool = new ForkJoinPool();
    }

    /**
     * Crawl a single website
     *
     * @param webSite  website
     * @param callback callback to process the result
     */
    public void crawl(WebSite webSite, ExtractionCompleted callback) {
        pool.execute(new RootEmailExtractionTask(webSite, maxLinksForPage, maxInheritance, callback));
    }

    public void crawl(List<WebSite> webSites, ExtractionCompleted callback) {
        webSites.forEach(webSite -> pool.execute(new RootEmailExtractionTask(webSite, maxLinksForPage, maxInheritance, callback)));
    }

    /**
     * Wait for the end of crawling process
     *
     * @param timeout waiting timeout
     * @param unit    waiting timeout unit
     * @return true if process finished
     */
    public boolean await(long timeout, TimeUnit unit) {
        return pool.awaitQuiescence(timeout, unit);
    }

    @Override
    public void close() {
        pool.shutdown();
    }

    public ForkJoinPool getPool() {
        return pool;
    }

    public int getMaxLinksForPage() {
        return maxLinksForPage;
    }

    public int getMaxInheritance() {
        return maxInheritance;
    }

}
