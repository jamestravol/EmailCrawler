package jt.upwork.crawler;

import jt.upwork.crawler.statistics.RuntimeStatistics;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final String[] skipDomainsList;
    private final String[] skipEmailsPatterns;
    private final UrlComparator urlComparator;

    private final ForkJoinPool pool;

    /**
     * Get the list of emails for the site
     *
     * @param website website
     * @return the list of emails (never null)
     */
    public static List<String> getEmails(String website) throws MalformedURLException {
        Crawler crawler = new Crawler(5, 1, 20);
        return new ArrayList<>(crawler.crawl(new WebSite(null, new URL(website))));
    }

    /**
     * Get the map of emails for the site list
     *
     * @param domains the website list
     * @return the map of emails for each website (never null)
     */
    public static Map<String, List<String>> getEmails(List<String> domains) throws MalformedURLException {
        final Crawler crawler = new Crawler(5, 1, 20);
        final Map<String, List<String>> result = crawler.crawl(domains);
        crawler.getPool().shutdown();
        try {
            crawler.getPool().awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException ignored) {
        }
        return result;
    }

    /**
     * Make the crawler
     *
     * @param maxLinksForPage maximum links that are processed for a single page
     * @param maxInheritance  maximum immersion while links following
     * @param threadCount     target parallelism level
     */
    public Crawler(int maxLinksForPage, int maxInheritance, int threadCount) {
        this(maxLinksForPage, maxInheritance, threadCount, new String[0], new String[0], new String[0]);
    }

    /**
     * Make the crawler
     *
     * @param maxLinksForPage      maximum links that are processed for a single page
     * @param maxInheritance       maximum immersion while links following
     * @param threadCount          target parallelism level
     * @param skipDomainsList      the list of  domains that should not be processed
     * @param skipEmailsPatterns   the patterns of emails that shouldn't be processed
     * @param priorityLinkContains the list of words that makes containing them links to be first processed
     */
    public Crawler(int maxLinksForPage, int maxInheritance, int threadCount, String[] skipDomainsList,
                   String[] skipEmailsPatterns, String[] priorityLinkContains) {
        this.maxLinksForPage = maxLinksForPage;
        this.maxInheritance = maxInheritance;
        this.pool = new ForkJoinPool(threadCount, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        this.skipDomainsList = skipDomainsList;
        this.skipEmailsPatterns = skipEmailsPatterns;
        this.urlComparator = UrlComparator.of(priorityLinkContains);
    }

    /**
     * Crawl a single website synchronously
     *
     * @param webSite website
     */
    public List<String> crawl(WebSite webSite) {
        return new ArrayList<>(pool.invoke(new RootEmailExtractionTask(webSite, this, null)));
    }

    /**
     * Crawl the map of emails for the site list
     *
     * @param domains the website list
     * @return the map of emails for each website (never null)
     */
    public Map<String, List<String>> crawl(List<String> domains) throws MalformedURLException {

        final ConcurrentHashMap<String, List<String>> result = new ConcurrentHashMap<>();

        for (String domain : domains) {
            pool.execute(new RootEmailExtractionTask(new WebSite(null, new URL(domain)), this,
                    (webSite, emails) -> result.put(webSite.getUrl().toString(), new ArrayList<>(emails))));
        }

        while (true) {
            if (pool.awaitQuiescence(1, TimeUnit.DAYS)) {
                break;
            }
        }

        return result;
    }

    /**
     * Crawls for emails
     *
     * @param webSites   the list of websites
     * @param statistics statistics object
     * @param callback   callback for the result processing
     */
    public void crawl(List<WebSite> webSites, RuntimeStatistics statistics, ExtractionCompleted callback) {
        webSites.forEach(webSite -> pool.execute(new RootEmailExtractionTask(webSite, this, statistics, callback)));
    }

    /**
     * Crawls for emails
     *
     * @param webSites the list of websites
     * @param callback callback for the result processing
     */
    public void crawl(List<WebSite> webSites, ExtractionCompleted callback) {
        webSites.forEach(webSite -> pool.execute(new RootEmailExtractionTask(webSite, this, callback)));
    }

    /**
     * Wait until only <code>tasksLeft</code>> tasks left in the pool
     *
     * @param tasksLeft amount of tasks (positive number)
     */
    public void await(int tasksLeft) {

        while (true) {
            if (pool.getQueuedSubmissionCount() < tasksLeft) {
                return;
            }
        }
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

    public String[] getSkipDomainsList() {
        return skipDomainsList;
    }

    public String[] getSkipEmailsPatterns() {
        return skipEmailsPatterns;
    }

    public UrlComparator getUrlComparator() {
        return urlComparator;
    }
}
