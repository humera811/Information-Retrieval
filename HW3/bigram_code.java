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
public class WordCountBigrams {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
   //private final static IntWritable one = new IntWritable(1);
    private final Text bigram = new Text();
    private final Text docId = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\\t", 2);

      docId.set(parts[0]);

      String split_string = parts[1].replaceAll("[^a-zA-Z]+", " ").toLowerCase(Locale.ROOT);
      StringTokenizer tokenizer = new StringTokenizer(split_string, " ");
      String firstWord = null;
      String secondWord = null;
      while (tokenizer.hasMoreTokens()) {
        String wordString = tokenizer.nextToken();

        if (!wordString.trim().isEmpty()) {
          if (firstWord == null) {
            firstWord = wordString;
            continue;
          } else if (secondWord == null) {
            secondWord = wordString;
          } else {
            firstWord = secondWord;
            secondWord = wordString;
          }
          firstWord = firstWord.trim();
          firstWord = firstWord.replaceAll("(\\r|\\n)", "");
          // System.out.println(firstWord);
          if ((firstWord.equals("computer") && secondWord.equals("science"))
              || (firstWord.equals("information") && secondWord.equals("retrieval"))
              || (firstWord.equals("power") && secondWord.equals("politics"))
              || (firstWord.equals("los") && secondWord.equals("angeles"))
              || (firstWord.equals("bruce") && secondWord.equals("willis"))) {
            bigram.set(String.format("%s %s", firstWord, secondWord));
            context.write(bigram, docId);
          }

        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    //private IntWritable result = new IntWritable();
    @Override
    public void reduce(Text bigram, Iterable<Text> docIds, Context context) throws IOException, InterruptedException {
      //logger.info("inside reducer\n");
      Map<String, Integer> dictionary = new HashMap<>();
      for (Text docId : docIds) {
        String docIdString = docId.toString();
        //logger.info("printing docIDs\n");
        //logger.info(docID);
        dictionary.put(docIdString, dictionary.getOrDefault(docIdString, 0) + 1);
      }
      //logger.info(docval.toString());
      StringBuilder bigram_freq = new StringBuilder();
      for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
        if (bigram_freq.length() > 0) {
          bigram_freq.append("\t");
        }
        String docId = entry.getKey();
        Integer bigramFrequency = entry.getValue();
        String docIdBigramFrequency = String.format("%s:%d", docId, bigramFrequency);
        bigram_freq.append(docIdBigramFrequency);
      }

      context.write(bigram, new Text(bigram_freq.toString()));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    
    String inputFile = args[0];
    String outputFile = args[1];

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Bigrams Inverted Index");

    job.setJarByClass(WordCountBigrams.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
   //job.setCombinerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    Path inputFilePath = new Path(inputFile);
    Path outputFilePath = new Path(outputFile);
    FileSystem fileSystem = outputFilePath.getFileSystem(conf);

    FileInputFormat.addInputPath(job, inputFilePath);
    FileOutputFormat.setOutputPath(job, outputFilePath);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
} // WordCount