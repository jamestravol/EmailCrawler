package jt.upwork.crawler;

import java.util.Set;

/**
 * A task for processing a root website.
 *
 * @author jamestravol
 */
public final class RootEmailExtractionTask extends EmailExtractionTask {

    private final WebSite webSite;
    private ExtractionCompleted extractionCompleted;

    public RootEmailExtractionTask(WebSite webSite, int maxLinksForPage, int maxInheritance, ExtractionCompleted extractionCompleted) {
        super(webSite.getUrl(), webSite.getUrl(), 0, maxLinksForPage, maxInheritance);
        this.webSite = webSite;
        this.extractionCompleted = extractionCompleted;
    }

    @Override
    protected Set<String> compute() {
        Set<String> result = super.compute();
        extractionCompleted.onComplete(webSite, result);
        return result;
    }
}
