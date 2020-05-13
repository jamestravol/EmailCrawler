package jt.upwork.crawler.script;

import jt.upwork.crawler.Crawler;
import jt.upwork.crawler.UrlUtils;
import jt.upwork.crawler.WebSite;
import jt.upwork.crawler.statistics.IgnoredCloseable;
import jt.upwork.crawler.statistics.RuntimeStatistics;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp.BasicDataSource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Script for database updating
 *
 * @author jamestravol
 */
@SuppressWarnings("unused")
public final class ProcessingScript {

    private static final Logger LOGGER = Logger.getLogger(ProcessingScript.class.getName());

    private final BasicDataSource dataSource = new BasicDataSource();

    private String incomeTableName;
    private String incomeIdField;
    private String incomeUrlField;

    private String outcomeTableName;
    private String outcomeIdField;
    private String outcomeEmailsField;
    private volatile int outcomeEmailsFieldLength;

    private int processingBatchSize;
    private int processingStartOffset;

    private int crawlerThreadsCount;
    private int crawlerMaxLinksForPage;
    private int crawlerMaxInheritance;
    private String crawlerSkipDomainsList;
    private String crawlerSkipEmailsPatterns;
    private String crawlerPriorityLinkContains;

    private volatile String outcomeSql;

    /**
     * Executing the process
     */
    void execute() throws IOException, InvocationTargetException, IllegalAccessException {

        final RuntimeStatistics statistics = new RuntimeStatistics();
        statistics.start();
        initFromProperties();

        outcomeSql = createOutcomeSql();

        final Crawler crawler = createCrawler();

        // startup the statistics timer. It logs the statistics every 300 mills to sout
        final Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                statistics.log(crawler.getPool());
            }
        }, 300, 300);

        LOGGER.info("Obtaining the database connection");

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {

            int step = 0;

            // thi flag shows if current batch contains any applicable items
            boolean hasItems = true;

            while (hasItems) {

                LOGGER.info(String.format("Requesting the batch step %d", step));

                List<WebSite> webSites = new ArrayList<>(processingBatchSize);

                final long mills = System.currentTimeMillis();

                try (final IgnoredCloseable ignored = statistics.getDataRequest().start(); final ResultSet resultSet = statement.executeQuery(createIncomeSql(step++))) {

                    hasItems = false;

                    while (resultSet.next()) {
                        hasItems = true;

                        // trying to form an url
                        final String url = resultSet.getString(incomeUrlField);
                        final Optional<URL> urlOptional = UrlUtils.makeUrl(url);
                        if (urlOptional.isPresent()) {
                            webSites.add(new WebSite(resultSet.getString(incomeIdField), urlOptional.get()));
                        } else {
                            LOGGER.severe(String.format("Unable to form URL from '%s'. Skipping...", url));
                        }
                    }
                    statistics.getDataRequest().success(System.currentTimeMillis() - mills);
                }

                if (!webSites.isEmpty()) {
                    LOGGER.info(String.format("Awaiting for %d tasks left in the crawler's queue", crawlerThreadsCount));
                    // when the queue is small enough we add new new batch
                    crawler.await(crawlerThreadsCount);
                    LOGGER.info(String.format("Crawling for new %s websites", webSites.size()));
                    crawler.crawl(webSites, statistics, this::onComplete);
                }

            }

            // wait at the end
            LOGGER.info("Awaiting for the crawler to finish");
            while (true) {
                if (crawler.getPool().awaitQuiescence(1, TimeUnit.DAYS)) {
                    break;
                }
            }

            crawler.getPool().shutdown();
            try {
                crawler.getPool().awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException ignored) {
            }

            timer.cancel();

            statistics.log(crawler.getPool());

        } catch (SQLException e) {
            LOGGER.severe(String.format("SQL exception occurred. Message: %s", e));
        }

    }

    private void initFromProperties() throws IOException, InvocationTargetException, IllegalAccessException {
        Properties properties = new Properties();
        properties.load(ProcessingScript.class.getResourceAsStream("/app.properties"));

        LOGGER.info("Initializing the datasource");
        BeanUtils.populate(dataSource, properties.entrySet()
                .stream().filter(entry -> entry.getKey().toString().startsWith("datasource."))
                .collect(Collectors.toMap(e -> e.getKey().toString().substring(11),
                        Map.Entry::getValue)));

        LOGGER.info("Initializing the script parameters");
        BeanUtils.populate(this, properties.entrySet()
                .stream().filter(entry -> entry.getKey().toString().startsWith("script."))
                .collect(Collectors.toMap(e -> e.getKey().toString().substring(7),
                        Map.Entry::getValue)));

    }

    private Crawler createCrawler() {

        LOGGER.info(String.format("Creating the Crawler with params - maxLinksForPage: %s, maxInheritance: %s, threadCount: %s",
                crawlerMaxLinksForPage, crawlerMaxInheritance, crawlerThreadsCount));
        return new Crawler(crawlerMaxLinksForPage,
                crawlerMaxInheritance,
                crawlerThreadsCount,
                getTrimmedArray(crawlerSkipDomainsList),
                getTrimmedArray(crawlerSkipEmailsPatterns),
                getTrimmedArray(crawlerPriorityLinkContains));

    }

    private String[] getTrimmedArray(String value) {
        final String[] skipDomains = value.split(",");

        for (int i = 0; i < skipDomains.length; i++) {
            skipDomains[i] = skipDomains[i].trim();
        }
        return skipDomains;
    }

    private void onComplete(WebSite webSite, Set<String> emails) {

        // form the string of valid length
        String result = String.join(",", emails);

        if (result.length() > outcomeEmailsFieldLength) {
            LOGGER.warning(String.format("Email resulting string is greater than %d symbols: %s", outcomeEmailsFieldLength, result));
            result = result.substring(0, Math.min(outcomeEmailsFieldLength, result.lastIndexOf(",")));
        }

        if (!result.isEmpty()) {
            LOGGER.info(String.format("Updating website %s. Emails: %s", webSite, result));
            try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(outcomeSql)) {
                statement.setString(1, result);
                statement.setString(2, webSite.getId());

                final int updated = statement.executeUpdate();
                LOGGER.info(String.format("Update of website %s performed. %d rows affected.", webSite, updated));
            } catch (SQLException e) {
                LOGGER.severe(String.format("SQL exception occurred. Message^ %s", e));
            }
        }

    }

    private String createIncomeSql(int step) {
        return String.format("SELECT %s, %s FROM %s ORDER BY %s LIMIT %d OFFSET %d",
                incomeIdField, incomeUrlField, incomeTableName, incomeIdField, processingBatchSize, processingStartOffset + processingBatchSize * step);
    }

    private String createOutcomeSql() {
        return String.format("UPDATE %s SET %s = ? WHERE %s = ?",
                outcomeTableName, outcomeEmailsField, outcomeIdField);
    }

    public String getIncomeTableName() {
        return incomeTableName;
    }

    public void setIncomeTableName(String incomeTableName) {
        this.incomeTableName = incomeTableName;
    }

    public String getIncomeIdField() {
        return incomeIdField;
    }

    public void setIncomeIdField(String incomeIdField) {
        this.incomeIdField = incomeIdField;
    }

    public String getIncomeUrlField() {
        return incomeUrlField;
    }

    public void setIncomeUrlField(String incomeUrlField) {
        this.incomeUrlField = incomeUrlField;
    }

    public String getOutcomeTableName() {
        return outcomeTableName;
    }

    public void setOutcomeTableName(String outcomeTableName) {
        this.outcomeTableName = outcomeTableName;
    }

    public String getOutcomeIdField() {
        return outcomeIdField;
    }

    public void setOutcomeIdField(String outcomeIdField) {
        this.outcomeIdField = outcomeIdField;
    }

    public String getOutcomeEmailsField() {
        return outcomeEmailsField;
    }

    public void setOutcomeEmailsField(String outcomeEmailsField) {
        this.outcomeEmailsField = outcomeEmailsField;
    }

    public int getOutcomeEmailsFieldLength() {
        return outcomeEmailsFieldLength;
    }

    public void setOutcomeEmailsFieldLength(int outcomeEmailsFieldLength) {
        this.outcomeEmailsFieldLength = outcomeEmailsFieldLength;
    }

    public int getProcessingBatchSize() {
        return processingBatchSize;
    }

    public void setProcessingBatchSize(int processingBatchSize) {
        this.processingBatchSize = processingBatchSize;
    }

    public int getProcessingStartOffset() {
        return processingStartOffset;
    }

    public void setProcessingStartOffset(int processingStartOffset) {
        this.processingStartOffset = processingStartOffset;
    }

    public int getCrawlerThreadsCount() {
        return crawlerThreadsCount;
    }

    public void setCrawlerThreadsCount(int crawlerThreadsCount) {
        this.crawlerThreadsCount = crawlerThreadsCount;
    }

    public int getCrawlerMaxLinksForPage() {
        return crawlerMaxLinksForPage;
    }

    public void setCrawlerMaxLinksForPage(int crawlerMaxLinksForPage) {
        this.crawlerMaxLinksForPage = crawlerMaxLinksForPage;
    }

    public int getCrawlerMaxInheritance() {
        return crawlerMaxInheritance;
    }

    public void setCrawlerMaxInheritance(int crawlerMaxInheritance) {
        this.crawlerMaxInheritance = crawlerMaxInheritance;
    }

    public String getCrawlerSkipDomainsList() {
        return crawlerSkipDomainsList;
    }

    public void setCrawlerSkipDomainsList(String crawlerSkipDomainsList) {
        this.crawlerSkipDomainsList = crawlerSkipDomainsList;
    }

    public String getCrawlerSkipEmailsPatterns() {
        return crawlerSkipEmailsPatterns;
    }

    public void setCrawlerSkipEmailsPatterns(String crawlerSkipEmailsPatterns) {
        this.crawlerSkipEmailsPatterns = crawlerSkipEmailsPatterns;
    }

    public String getCrawlerPriorityLinkContains() {
        return crawlerPriorityLinkContains;
    }

    public void setCrawlerPriorityLinkContains(String crawlerPriorityLinkContains) {
        this.crawlerPriorityLinkContains = crawlerPriorityLinkContains;
    }
}
