import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


public class BasicCrawler extends WebCrawler {
    private final static String fetchPath = "data/crawl/fetch_wsj.csv";
    private final static String visitPath = "data/crawl/visit_wsj.csv";
    private final static String urlPath = "data/crawl/urls_wsj.csv";
    CrawlStat crawlStat;
    private CsvWriter cw, csvVisit, csvUrls;
    private File fetch_csv, visit_csv, urls_csv;
    private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|json|xml||mid|mp2|mp3|mp4|rar|zip|gz|wav|avi|mov|mpeg|woff2|ttf|webmanifest))$");

    private String indicator(String str) {
        if(str.startsWith("http://www.wsj.com") || str.startsWith("https://www.wsj.com")) return "a";
        else return "b";
    }
    // public BasicCrawler() {
    //     // this.numSeenImages = 0;
    //     try {
    //       csvFetch = new CSVWriter(new FileWriter("fetch_wsj.csv", true));
    //       csvVisit = new CSVWriter(new FileWriter("visit_wsj.csv", true));
    //       csvUrls = new CSVWriter(new FileWriter("urls_wsj.csv", true))
    //     } catch (Exception e) {
    //       e.getStackTrace();
    //     }
    //   }
    public BasicCrawler() throws IOException {
        crawlStat = new CrawlStat();

        fetch_csv = new File(fetchPath);
        visit_csv = new File(visitPath);
        urls_csv = new File(urlPath);
        if (fetch_csv.isFile()) fetch_csv.delete();
        if (visit_csv.isFile()) visit_csv.delete();
        if(urls_csv.isFile()) urls_csv.delete();

        cw = new CsvWriter(new FileWriter(fetch_csv, true), ',');
        cw.write("URL");
        cw.write("Status");
        cw.endRecord();
        cw. close();

        csvVisit = new CsvWriter(new FileWriter(visit_csv, true), ',');
        csvVisit.write("URL");
        csvVisit.write("Size(Bytes)");
        csvVisit.write("Number of outlinks found");
        csvVisit.write("Content-type");
        csvVisit.endRecord();
        csvVisit.close();

        csvUrls = new CsvWriter(new FileWriter(urls_csv, true), ',');
        csvUrls.write("URL");
        csvUrls.write("Indicator");
        csvUrls.endRecord();
        csvUrls. close();

    }
// implementing visit and shouldvisit

  /**
   * You should implement this function to specify whether the given url
   * should be crawled or not (based on your crawling logic).
   */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        // if (IMAGE_EXTENSIONS.matcher(href).matches()) {
        //     // numSeenImages.incrementAndGet();
        //     return false;
        return !FILTERS.matcher(href).matches() // only pages in coursesite
                && (href.startsWith("http://www.wsj.com") || href.startsWith("https://www.wsj.com"));
    }

    /**
     * This function is called when a page is fetched and ready 
     * to be processed 
     * by your program.
     */
    @Override
    public void visit(Page page) {
        String url = page.getWebURL().getURL();
        System.out.println("URL: " + url);

        String contentType = page.getContentType();
        if(contentType.startsWith("text")) contentType = contentType.split(";")[0];
        Map<String, Integer> type = crawlStat.getType();
        type.put(contentType, type.getOrDefault(contentType, 0) + 1);
        int fileSize = page.getContentData().length;
        crawlStat.setSize(fileSize);


        
        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();
            crawlStat.incUrls(links.size());
            for(WebURL link : links) {
                String str = link.getURL();
                if(str.startsWith("http")) str = str.split("//")[1];
                if(str.startsWith("www.")) str = str.substring(4);
                crawlStat.myUrlset().add(str);
            }

            try {
                // csvFetch = new CSVWriter(new FileWriter("fetch_wsj.csv", true));
                // csvVisit = new CSVWriter(new FileWriter("visit_wsj.csv", true));
                // csvUrls = new CSVWriter(new FileWriter("urls_wsj.csv", true));
                csvVisit = new CsvWriter(new FileWriter(visit_csv, true), ',');
                csvUrls = new CsvWriter(new FileWriter(urls_csv, true), ',');
                csvVisit.write(url);
                csvVisit.write(String.valueOf(fileSize));
                csvVisit.write(String.valueOf(links.size()));
                csvVisit.write(contentType);
                csvVisit.endRecord();
                csvVisit.close();

                
                for(WebURL link : links) {
                    csvUrls.write(link.getURL());
                    String str = link.getURL().toLowerCase();
                    csvUrls.write(indicator(str));
                    csvUrls.endRecord();
                    // if (visit[3].contains("image")) {
                    //     System.out.println(visit[0] + " " + visit[1] + " " + visit[2] + " " + visit[3]);
                    //   }
                }
                csvUrls.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            try {
                csvVisit = new CsvWriter(new FileWriter(visit_csv, true), ',');
                csvVisit.write(url);
                csvVisit.write(String.valueOf(fileSize));
                csvVisit.write("0");
                csvVisit.write(contentType);
                csvVisit.endRecord();
                csvVisit. close();
            //     csvFetch.close();
            //     csvUrls.close();
            //     csvVisit.close();
            //   } catch (Exception e) {
            //     e.getStackTrace();
            //   }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
                int docid = page.getWebURL().getDocid();
                String url = page.getWebURL().getURL();
                String domain = page.getWebURL().getDomain();
                String path = page.getWebURL().getPath();
                String subDomain = page.getWebURL().getSubDomain();
                String parentUrl = page.getWebURL().getParentUrl();
                String anchor = page.getWebURL().getAnchor();
                int size = page.getContentData().length;
                int statusCode = page.getStatusCode();

    protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
        crawlStat.incAttFetch();
        if(statusCode < 300) crawlStat.incSucFetch();
        else crawlStat.incFailFetch();
        Map<Integer, Integer> statusMap = crawlStat.getStatusCode();
        statusMap.put(statusCode, statusMap.getOrDefault(statusCode, 0) + 1);
        try {
            cw = new CsvWriter(new FileWriter(fetch_csv, true), ',');
            cw.write(webUrl.getURL());
            cw.write(String.valueOf(statusCode));
            cw.endRecord();
            cw. close();
            // csvFetch.close();
            // csvUrls.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void onUnhandledException(WebURL webUrl, Throwable e) {
        crawlStat.incFailFetch();
    }

    protected void onPageBiggerThanMaxSize(String urlStr, long pageSize) {
        crawlStat.incFailFetch();
        crawlStat.incAttFetch();
    }

    protected void onRedirectedStatusCode(Page page) {
        try {
            csvUrls = new CsvWriter(new FileWriter(urls_csv, true), ',');
            csvUrls.write(page.getRedirectedToUrl());
            csvUrls.write(indicator(page.getRedirectedToUrl().toLowerCase()));
            csvUrls.endRecord();
            csvUrls. close();

            String str = page.getRedirectedToUrl();
            if(str.startsWith("http")) str = str.split("//")[1];
            if(str.startsWith("www.")) str = str.substring(4);
            crawlStat.myUrlset().add(str);
            // csvFetch.close();
            // csvUrls.close();

        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object getMyLocalData() {
        return crawlStat;
    }
}