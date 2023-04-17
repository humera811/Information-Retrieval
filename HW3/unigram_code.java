import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import java.util.logging.Level;
//import java.util.logging.Logger;

public class WordCount {
    private static final Logger logger = Logger.getLogger(WordCount.class.getName());
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    //private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    private final Text docId = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] split_string = value.toString().split("\\t", 2);

      docId.set(split_string[0]);

      String wordsString = split_string[1].replaceAll("[^a-zA-Z]+", " ").toLowerCase(Locale.ROOT);
      StringTokenizer itr = new StringTokenizer(wordsString, " ");
      //logger.info("here\n");
      while (itr.hasMoreTokens()) {
        String wordString = itr.nextToken();
        if (!wordString.trim().isEmpty()) {
          word.set(wordString);

          context.write(word, docId);
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    //private IntWritable result = new IntWritable();
    @Override
    public void reduce(Text word, Iterable<Text> docIds, Context context) throws IOException, InterruptedException {
      Map<String, Integer> dictionary = new HashMap<>();
      for (Text docId : docIds) {
        String docIdString = docId.toString();
        dictionary.put(docIdString, dictionary.getOrDefault(docIdString, 0) + 1);
      }

      StringBuilder freq = new StringBuilder();
      for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
        if (freq.length() > 0) {
          freq.append("\t");
        }
        String docId = entry.getKey();
        Integer word_freq = entry.getValue();
        String docIdword_freq = String.format("%s:%d", docId, word_freq);
        freq.append(docIdword_freq);
      }

      context.write(word, new Text(freq.toString()));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    String inputFile = args[0];
    String outputFile = args[1];

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}// WordCount