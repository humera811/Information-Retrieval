import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

import java.io.File;
import java.util.*;

import static edu.uci.ics.crawler4j.robotstxt.UserAgentDirectives.logger;

public class Controller {
    public static void main(String[] args) throws Exception {
        String crawlStorageFolder = "/data/crawl";
        File crawlStorage = new File("data/crawl");
        int numberOfCrawlers = 20;
        config.setMaxDepthOfCrawling(16);
        config.setMaxPagesToFetch(20000);
        // int maxDepthOfCrawling = 16;
        // int maxPagesToFetch = 20000;
        int politenessDelay = 500;
        String userAgentString = "User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36";

        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(crawlStorage.getAbsolutePath());
        config.setIncludeBinaryContentInCrawling(true);
        config.setPolitenessDelay(politenessDelay);
        config.setUserAgentString(userAgentString);
               /*
         * Instantiate the controller for this crawl.
         */
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
          /*
         * For each crawl, you need to add some seed urls. These are the first
         * URLs that are fetched and then the crawler starts following links
         * which are found in these pages
         */
        controller.addSeed("https://www.wsj.com");
         /*
         * Start the crawl. This is a blocking operation, meaning that your code
         * will reach the line after this only when crawling is finished.
         */
        controller.start(BasicCrawler.class, numberOfCrawlers);

        List<Object> crawlersLocalData = controller.getCrawlersLocalData();
        int total_attribs = 0;
        int total_succ = 0;
        int total_failF = 0;
        long total_url = 0;
        long totalInUrls = 0;
        long totalOutUrls = 0;

        Map<Integer, Integer> statusMap = new HashMap<>();
        Map<String, Integer> typeMap = new HashMap<>();
        Set<String> urlSet = new HashSet<>();
        long[] totalSize = new long[5];
        for (Object localData : crawlersLocalData) {
            CrawlStat stat = (CrawlStat)localData;
            total_attribs += stat.myAttFetch();
            total_succ += stat.mySucFetch();
            total_failF += stat.myFailFetch();
            total_url += stat.myUrls();
            for(Map.Entry<Integer, Integer> entry : stat.getStatusCode().entrySet()) {                                         
                statusMap.put(entry.getKey(), statusMap.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
            for(Map.Entry<String, Integer> entry : stat.getType().entrySet()) {
                typeMap.put(entry.getKey(), typeMap.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
            long[] size = stat.getSize();
            for(int i = 0; i < size.length; i++) {
                totalSize[i] += size[i];
            }

            urlSet.addAll(stat.myUrlset());
        }
        for(String url : urlSet) {
            if(url.startsWith("wsj.com")) {
                totalInUrls++;
            }else totalOutUrls++;
        }
        // stats data 
        logger.info("Aggregated Statistics:");
        // attempted fetch
        logger.info("\tTotal attempted fetch: {}", total_attribs);
        // succeded fetch
        logger.info("\tTotal succeeded fetch: {}", total_succ);
        // failed fetch
        logger.info("\tTotal failed fetch: {}", total_failF);
        // url totals
        logger.info("\tTotal URLs: {}", total_url);
        //unique urls
        logger.info("\tTotal unique URLs: {}", urlSet.size());
        //in urls
        logger.info("\tTotal inURLs: {}", totalInUrls);
        //out urls
        logger.info("\tTotal outURLs: {}", totalOutUrls);

        for(Map.Entry<Integer, Integer> entry : statusMap.entrySet()) {
            logger.info("\tStatus Code: {}, {}", entry.getKey(), entry.getValue());
        }
        for(Map.Entry<String, Integer> entry : typeMap.entrySet()) {
            logger.info("\tContent-Type: {}, {}", entry.getKey(), entry.getValue());
        }
        for(long size : totalSize) {
            logger.info("\tSize: {}", size);
        }
    }
}

