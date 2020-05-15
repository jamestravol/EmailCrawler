package jt.upwork.crawler;

import jt.upwork.crawler.statistics.IgnoredCloseable;
import jt.upwork.crawler.statistics.RuntimeStatistics;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A task for processing a sublink.
 *
 * @author jamestravol
 */
public class EmailExtractionTask extends RecursiveTask<Set<String>> {

    private static final Logger LOGGER = Logger.getLogger(EmailExtractionTask.class.getName());

    private static final Pattern singleEmailPattern = Pattern.compile("\\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,4}\\b",
            Pattern.CASE_INSENSITIVE);

    private final URL rootDomain;
    private final URL fullLink;
    private final Crawler crawler;
    private final int currentInheritance;
    protected final RuntimeStatistics statistics;
    private final ConcurrentLinkedQueue<URL> processedUrls;

    protected EmailExtractionTask(URL rootDomain, URL fullLink, int currentInheritance, Crawler crawler, RuntimeStatistics statistics) {
        this(rootDomain, fullLink, currentInheritance, crawler, statistics, new ConcurrentLinkedQueue<URL>());
    }

    private EmailExtractionTask(URL rootDomain, URL fullLink, int currentInheritance, Crawler crawler, RuntimeStatistics statistics,
                                ConcurrentLinkedQueue<URL> processedUrls) {
        this.rootDomain = rootDomain;
        this.fullLink = fullLink;
        this.currentInheritance = currentInheritance;
        this.crawler = crawler;
        this.statistics = statistics;
        this.processedUrls = processedUrls;
    }

    @Override
    protected Set<String> compute() {
        long mills = System.currentTimeMillis();
        try (final IgnoredCloseable ignored = statistics.getPage().start()) {
            Set<String> result = computeInternal();
            statistics.getPage().success(System.currentTimeMillis() - mills);
            return result;
        } catch (IOException | RuntimeException e) {
            LOGGER.severe(String.format("Exception occurred during request to URL: %s. Message: %s", fullLink, e));
            return Collections.emptySet();
        }
    }

    private Set<String> computeInternal() throws IOException {
        LinkedList<EmailExtractionTask> tasks = new LinkedList<>();

        Set<String> result = computeTheResult(tasks, fullLink);
        addResultsFromTasks(result, tasks);
        LOGGER.info(String.format("For url %s we got %s", fullLink, result));

        return result;
    }

    private Set<String> computeTheResult(LinkedList<EmailExtractionTask> tasks, URL fullLink) throws IOException {
        LOGGER.info(String.format("Processing the URL %s", fullLink));

        Connection.Response response;

        long mills = System.currentTimeMillis();
        try (final IgnoredCloseable ignored = statistics.getWebRequest().start()) {
            response = Jsoup.connect(fullLink.toString()).ignoreHttpErrors(true).method(Connection.Method.GET).timeout(300000).execute();
            statistics.getWebRequest().success(System.currentTimeMillis() - mills);
        }

        Document document;

        mills = System.currentTimeMillis();
        try (final IgnoredCloseable ignored = statistics.getParsing().start()) {
            document = response.parse();
            statistics.getParsing().success(System.currentTimeMillis() - mills);
        }


        Set<String> result;

        mills = System.currentTimeMillis();
        try (final IgnoredCloseable ignored = statistics.getProcessing().start()) {
            result = processingInternal(tasks, document);
            statistics.getProcessing().success(System.currentTimeMillis() - mills);
        }
        return result;
    }

    private Set<String> processingInternal(LinkedList<EmailExtractionTask> tasks, Document document) {
        List<String> hrefs = document.select("a[href]").stream().map(element -> element.attr("href"))
                .sorted(crawler.getUrlComparator()).collect(Collectors.toList());

        // keep the order
        Set<String> result = new LinkedHashSet<>();

        int processesLinks = 0;

        for (String href : hrefs) {
            // we check mailto for each link
            if (href.startsWith("mailto:")) {
                Matcher matcher = singleEmailPattern.matcher(href);
                while (matcher.find()) {
                    final String email = matcher.group();
                    if (emailCompatible(email)) {
                        LOGGER.fine(String.format("Got email %s from 'mailto' tag", email));
                        result.add(email);
                    }
                }
            } else if (processesLinks < crawler.getMaxLinksForPage() && currentInheritance < crawler.getMaxInheritance()) {
                // we process only maxLinksForPage amount of links and if inheritance in not exceeded
                if (href.startsWith("//")) {
                    final String tail = href.substring(1);
                    final Optional<URL> urlOptional = UrlUtils.makeUrl(rootDomain, tail);
                    if (urlOptional.isPresent()) {
                        URL url = urlOptional.get();
                        if (!processedUrls.contains(url)) {
                            // racing could be here but it's acceptable because more important to avoid blocking
                            processedUrls.offer(url);
                            if (websiteCompatible(url)) {
                                LOGGER.fine(String.format("Adding task for a //-task link. URL: %s", url));
                                forkOrExecute(tasks, url);
                                processesLinks++;
                            }
                        }
                    } else {
                        LOGGER.severe(String.format("Unable to concatenate URL parts '%s' and '%s'", rootDomain, tail));
                    }
                } else if (href.startsWith("/")) {
                    final Optional<URL> urlOptional = UrlUtils.makeUrl(rootDomain, href);
                    if (urlOptional.isPresent()) {
                        URL url = urlOptional.get();
                        if (!processedUrls.contains(url)) {
                            // racing could be here but it's acceptable because more important to avoid blocking
                            processedUrls.offer(url);
                            if (websiteCompatible(url)) {
                                LOGGER.fine(String.format("Adding task for a /-type link. URL: %s", url));
                                forkOrExecute(tasks, url);
                                processesLinks++;
                            }
                        }
                    } else {
                        LOGGER.severe(String.format("Unable to concatenate URL parts '%s' and '%s'", rootDomain, href));
                    }
                } else if (href.startsWith(rootDomain.getHost())) {
                    final Optional<URL> urlOptional = UrlUtils.makeUrl(href);
                    if (urlOptional.isPresent()) {
                        URL url = urlOptional.get();
                        if (!processedUrls.contains(url)) {
                            // racing could be here but it's acceptable because more important to avoid blocking 
                            processedUrls.offer(url);
                            if (websiteCompatible(url)) {
                                LOGGER.fine(String.format("Adding task for a full link. URL: %s", url));
                                forkOrExecute(tasks, url);
                                processesLinks++;
                            }
                        }
                    } else {
                        LOGGER.severe(String.format("Unable to form URL from '%s'", href));
                    }
                }
            }
        }

        for (Element element : document.getAllElements()) {

            String text = element.ownText().trim();

            Matcher matcher = singleEmailPattern.matcher(text);
            while (matcher.find()) {
                final String email = matcher.group();
                if (emailCompatible(email)) {
                    LOGGER.fine(String.format("Got email %s from email regexp", email));
                    result.add(email);
                }
            }
        }
        return result;
    }

    private void forkOrExecute(LinkedList<EmailExtractionTask> tasks, URL url) {
        final EmailExtractionTask task = new EmailExtractionTask(rootDomain, url, currentInheritance + 1,
                crawler, statistics, processedUrls);
        task.fork();
        tasks.add(task);
    }

    private boolean websiteCompatible(URL url) {
        for (String skipDomain : crawler.getSkipDomainsList()) {
            if (url.getHost().equalsIgnoreCase(skipDomain)) {
                return false;
            }
        }
        return true;
    }

    private boolean emailCompatible(String email) {
        for (String pattern : crawler.getSkipEmailsPatterns()) {
            if (email.matches(pattern)) {
                return false;
            }
        }
        return true;
    }

    private void addResultsFromTasks(Set<String> list, LinkedList<EmailExtractionTask> tasks) {
        final Iterator<EmailExtractionTask> iterator = tasks.descendingIterator();
        while (iterator.hasNext()) {
            list.addAll(iterator.next().join());
        }
    }

}
