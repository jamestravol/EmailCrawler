package jt.upwork.crawler;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.LogManager;

public class CrawlerTest {

    @BeforeClass
    public static void init() throws IOException {
        InputStream configFile = CrawlerTest.class.getResourceAsStream("/logger.properties");
        LogManager.getLogManager().readConfiguration(configFile);
    }

    @Test
    public void testSingleWebsite() throws IOException {
        final List<String> emails = Crawler.getEmails("https://support.google.com/mail/answer/22370?hl=e");
        Assert.assertTrue(emails.size() > 0);
    }

    @Test
    public void testMultipleWebsites() throws IOException {

        List<String> websites = new ArrayList<>();
        websites.add("https://support.google.com/mail/answer/22370?hl=en");
        websites.add("https://sparkmailapp.com/formal-email-template");
        websites.add("https://www.rapidtables.com/web/html/mailto.html");

        final Map<String, List<String>> result = Crawler.getEmails(websites);
        Assert.assertEquals(3, result.size());

        for (String website : websites) {
            Assert.assertTrue(result.containsKey(website));
            Assert.assertTrue(result.get(website).size() > 0);
        }
    }

}
