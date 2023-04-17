import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CrawlStat {
    private int attFetch;
    private int sucFetch;
    private int failFetch;
    private long urls;
    private long uniUrls;
    private long inUrls;
    private long outUrls;
    private Map<Integer, Integer> statusCode;
    private Map<String, Integer> type;
    private Set<String> urlSet;
    private long[] size;

    public CrawlStat() {
        attFetch = 0;
        sucFetch = 0;
        failFetch = 0;
        urls = 0;
        uniUrls = 0;
        inUrls = 0;
        outUrls = 0;
        statusCode = new HashMap<>();
        type = new HashMap<>();
        urlSet = new HashSet<>();
        size = new long[5];
    }

    public int myAttFetch() {  return attFetch; }

    public int mySucFetch() { return sucFetch; }

    public int myFailFetch() { return failFetch; }

    public long myUrls() { return urls; }

    public long myUniUrls() {   return uniUrls; }

    public long getInUrls() { return inUrls; }

    public long getOutUrls() { return outUrls; }

    public Map<Integer, Integer> getStatusCode() { return statusCode; }

    public Map<String, Integer> getType() { return type; }

    public Set<String> myUrlset() { return urlSet; }

    public long[] getSize() { return size; }

    public void incAttFetch() { this.attFetch++; }

    public void incSucFetch() { this.sucFetch++; }

    public void incFailFetch() { this.failFetch++; }

    public void incUrls(long urls) { this.urls += urls; }

    public void incUniUrls(long uniUrls) { this.uniUrls += uniUrls; }

    public void incInUrls(long inUrls) { this.inUrls += inUrls; }

    public void incOutUrls(long outUrls) { this.outUrls += outUrls; }

    public void setSize(int fileSize) {
        fileSize = fileSize/1024;
        if(fileSize < 1) this.size[0]++;
        else if(fileSize < 10) this.size[1]++;
        else if(fileSize < 100) this.size[2]++;
        else if(fileSize < 1024) this.size[3]++;
        else this.size[4]++;
    }
}
