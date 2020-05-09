package jt.upwork.crawler;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

public class CrawlerTest {

    @Ignore
    @Test
    public void testCrawler() throws IOException {

        InputStream configFile = CrawlerTest.class.getResourceAsStream("/logger.properties");
        LogManager.getLogManager().readConfiguration(configFile);

        List<WebSite> domains = new ArrayList<>();
        domains.add(new WebSite("0", UrlUtils.makeUrl("https://support.google.com/mail/answer/22370?hl=en").get()));
        domains.add(new WebSite("0", UrlUtils.makeUrl("https://sparkmailapp.com/formal-email-template").get()));
        domains.add(new WebSite("0", UrlUtils.makeUrl("https://www.rapidtables.com/web/html/mailto.html").get()));

        Crawler crawler = new Crawler(5, 5, 30);

        crawler.crawl(domains, (webSite, emails) -> System.out.println(webSite.toString() + emails));
        crawler.crawl(domains, (webSite, emails) -> System.out.println(webSite.toString() + emails));

        while (!crawler.await(1, TimeUnit.SECONDS)) {
            System.out.printf("******************************************\n");
            System.out.printf("Main: Parallelism: %d\n", crawler.getPool().getParallelism());
            System.out.printf("Main: Active Threads: %d\n", crawler.getPool().getActiveThreadCount());
            System.out.printf("Main: Task Count: %d\n", crawler.getPool().getQueuedTaskCount());
            System.out.printf("Main: Steal Count: %d\n", crawler.getPool().getStealCount());
            System.out.printf("******************************************\n");
        }

    }

}
