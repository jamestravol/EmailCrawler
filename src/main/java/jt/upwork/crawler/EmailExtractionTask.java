package jt.upwork.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final int currentInheritance;
    private final int maxLinksForPage;
    private final int maxInheritance;
    private final ConcurrentMap<URL, Object> processedUrls;

    public EmailExtractionTask(URL rootDomain, URL fullLink, int currentInheritance, int maxLinksForPage, int maxInheritance) {
        this(rootDomain, fullLink, currentInheritance, maxLinksForPage, maxInheritance, new ConcurrentHashMap<>());
    }

    private EmailExtractionTask(URL rootDomain, URL fullLink, int currentInheritance, int maxLinksForPage, int maxInheritance,
                                ConcurrentMap<URL, Object> processedUrls) {
        this.rootDomain = rootDomain;
        this.fullLink = fullLink;
        this.currentInheritance = currentInheritance;
        this.maxLinksForPage = maxLinksForPage;
        this.maxInheritance = maxInheritance;
        this.processedUrls = processedUrls;
    }

    @Override
    protected Set<String> compute() {
        try {
            return computeInternal();
        } catch (IOException | RuntimeException e) {
            LOGGER.severe(String.format("Exception occurred during request to URL: %s. Message: %s", fullLink, e));
            return Collections.emptySet();
        }
    }

    private Set<String> computeInternal() throws IOException {
        // keep the order
        Set<String> result = new LinkedHashSet<>();

        // unblocking checking
        if (processedUrls.putIfAbsent(fullLink, new Object()) != null) {
            LOGGER.fine(String.format("URL %s already processed", fullLink));
            return result;
        }

        LOGGER.info(String.format("Processing the URL %s", fullLink));

        Document document = Jsoup.connect(fullLink.toString()).ignoreHttpErrors(true).get();

        List<String> hrefs = document.select("a[href]").stream().map(element -> element.attr("href"))
                .sorted(UrlComparator.INSTANCE).collect(Collectors.toList());

        LinkedList<EmailExtractionTask> tasks = new LinkedList<>();

        int processesLinks = 0;

        for (String href : hrefs) {
            // we check mailto for each link
            if (href.startsWith("mailto:")) {
                Matcher matcher = singleEmailPattern.matcher(href);
                while (matcher.find()) {
                    LOGGER.fine(String.format("Got email %s from 'mailto' tag", matcher.group()));
                    result.add(matcher.group());
                }
            } else if (processesLinks < maxLinksForPage && currentInheritance < maxInheritance) {
                // we process only maxLinksForPage amount of links and if inheritance in not exceeded
                if (href.startsWith("//")) {
                    final String tail = href.substring(1);
                    final Optional<URL> urlOptional = UrlUtils.makeUrl(rootDomain, tail);
                    if (urlOptional.isPresent()) {
                        URL url = urlOptional.get();
                        LOGGER.fine(String.format("Following a // link. URL: %s", url));
                        EmailExtractionTask task = new EmailExtractionTask(rootDomain, url, currentInheritance + 1,
                                maxLinksForPage, maxInheritance, processedUrls);
                        task.fork();
                        tasks.add(task);
                        processesLinks++;
                    } else {
                        LOGGER.severe(String.format("Unable to concatenate URL parts '%s' and '%s'", rootDomain, tail));
                    }
                } else if (href.startsWith("/")) {
                    final Optional<URL> urlOptional = UrlUtils.makeUrl(rootDomain, href);
                    if (urlOptional.isPresent()) {
                        URL url = urlOptional.get();
                        LOGGER.fine(String.format("Following a / link. URL: %s", url));
                        EmailExtractionTask task = new EmailExtractionTask(rootDomain, url, currentInheritance + 1,
                                maxLinksForPage, maxInheritance, processedUrls);
                        task.fork();
                        tasks.add(task);
                        processesLinks++;
                    } else {
                        LOGGER.severe(String.format("Unable to concatenate URL parts '%s' and '%s'", rootDomain, href));
                    }
                } else if (href.startsWith(rootDomain.getHost())) {
                    final Optional<URL> urlOptional = UrlUtils.makeUrl(href);
                    if (urlOptional.isPresent()) {
                        URL url = urlOptional.get();
                        LOGGER.fine(String.format("Following a full link. URL: %s", url));
                        EmailExtractionTask task = new EmailExtractionTask(rootDomain, url, currentInheritance + 1,
                                maxLinksForPage, maxInheritance, processedUrls);
                        task.fork();
                        tasks.add(task);
                        processesLinks++;
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
                LOGGER.fine(String.format("Got email %s from email regexp", matcher.group()));
                result.add(matcher.group());
            }
        }

        addResultsFromTasks(result, tasks);

        LOGGER.info(String.format("For url %s we got %s", fullLink, result));

        return result;
    }

    private void addResultsFromTasks(Set<String> list, List<EmailExtractionTask> tasks) {
        for (EmailExtractionTask item : tasks) {
            list.addAll(item.join());
        }
    }

}
