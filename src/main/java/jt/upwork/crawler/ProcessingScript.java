package jt.upwork.crawler;

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
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Script for database updating
 *
 * @author jamestravol
 */
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
    private long processingInfoMessageTimeoutMills;

    private int crawlerThreadsCount;
    private int crawlerMaxLinksForPage;
    private int crawlerMaxInheritance;

    private volatile String outcomeSql;

    /**
     * Executing the process
     *
     * @throws IOException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    void execute() throws IOException, InvocationTargetException, IllegalAccessException {

        initFromProperties();

        outcomeSql = createOutcomeSql();

        Crawler crawler = createCrawler();

        LOGGER.info("Obtainint the database connection");

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {

            int step = 0;

            boolean hasItems = true;

            while (hasItems) {

                LOGGER.info(String.format("Requesting the batch step %d", step));

                try (final ResultSet resultSet = statement.executeQuery(createIncomeSql(step++))) {

                    List<WebSite> webSites = new ArrayList<>(processingBatchSize);

                    hasItems = false;

                    while (resultSet.next()) {
                        hasItems = true;

                        final String url = resultSet.getString(incomeUrlField);
                        final Optional<URL> urlOptional = UrlUtils.makeUrl(url);
                        if (urlOptional.isPresent()) {
                            webSites.add(new WebSite(resultSet.getString(incomeIdField), urlOptional.get()));
                        } else {
                            LOGGER.severe(String.format("Unable to form URL from '%s'. Skipping...", url));
                        }
                    }

                    if (!webSites.isEmpty()) {
                        waitForCrawler(crawler);
                        LOGGER.info(String.format("Crawling for new %s websites", webSites.size()));
                        // when crawler finishes the batch we add a new one
                        crawler.crawl(webSites, this::onComplete);
                    }
                }

            }

            // wait at the end
            waitForCrawler(crawler);

        } catch (SQLException e) {
            LOGGER.severe(String.format("SQL exception occurred. Message: %s", e));
        }

    }

    private void waitForCrawler(Crawler crawler) {
        LOGGER.info("Awaiting for the crawler to be free");

        // wait for he crawler is free
        while (!crawler.await(this.processingInfoMessageTimeoutMills, TimeUnit.MILLISECONDS)) {
            LOGGER.info(String.format("Crawling in process. Parallelism: %d. Active threads: %d. Queued task count: %d",
                    crawler.getPool().getParallelism(), crawler.getPool().getActiveThreadCount(),
                    crawler.getPool().getQueuedTaskCount()));
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
        if (crawlerThreadsCount == 0) {
            LOGGER.info(String.format("Creating the Crawler with params - maxLinksForPage: %s, maxInheritance: %s",
                    crawlerMaxLinksForPage, crawlerMaxInheritance));
            return new Crawler(crawlerMaxLinksForPage, crawlerMaxInheritance);
        } else {
            LOGGER.info(String.format("Creating the Crawler with params - maxLinksForPage: %s, maxInheritance: %s, threadCount: %s",
                    crawlerMaxLinksForPage, crawlerMaxInheritance, crawlerThreadsCount));
            return new Crawler(crawlerMaxLinksForPage, crawlerMaxInheritance, crawlerThreadsCount);
        }
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

    public long getProcessingInfoMessageTimeoutMills() {
        return processingInfoMessageTimeoutMills;
    }

    public void setProcessingInfoMessageTimeoutMills(long processingInfoMessageTimeoutMills) {
        this.processingInfoMessageTimeoutMills = processingInfoMessageTimeoutMills;
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
}
