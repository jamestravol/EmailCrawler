package jt.upwork.crawler;

import jt.upwork.crawler.statistics.IgnoredCloseable;
import jt.upwork.crawler.statistics.RuntimeStatistics;

import java.util.Set;

/**
 * A task for processing a root website.
 *
 * @author jamestravol
 */
public final class RootEmailExtractionTask extends EmailExtractionTask {

    private final WebSite webSite;
    private ExtractionCompleted extractionCompleted;

    public RootEmailExtractionTask(WebSite webSite, Crawler crawler, RuntimeStatistics statistics,
                                   ExtractionCompleted extractionCompleted) {
        super(webSite.getUrl(), webSite.getUrl(), 0, crawler, statistics);
        this.webSite = webSite;
        this.extractionCompleted = extractionCompleted;
    }

    public RootEmailExtractionTask(WebSite webSite, Crawler crawler,
                                   ExtractionCompleted extractionCompleted) {
        super(webSite.getUrl(), webSite.getUrl(), 0, crawler, new RuntimeStatistics());
        this.webSite = webSite;
        this.extractionCompleted = extractionCompleted;
    }

    @Override
    protected Set<String> compute() {
        final long mills = System.currentTimeMillis();
        try (final IgnoredCloseable ignored = statistics.getWebsite().start()) {
            Set<String> result = super.compute();
            processCallback(result);
            statistics.getWebsite().success(System.currentTimeMillis() - mills);
            return result;
        }
    }

    private void processCallback(Set<String> result) {
        if (extractionCompleted != null) {
            final long mills = System.currentTimeMillis();
            try (final IgnoredCloseable ignored = statistics.getCallback().start()) {
                statistics.addEmails(result.size());
                extractionCompleted.onComplete(webSite, result);
                statistics.getCallback().success(System.currentTimeMillis() - mills);
            }
        }
    }
}
