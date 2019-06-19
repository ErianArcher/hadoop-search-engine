package scut.se.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import scut.se.mapred.completefile.CompleteFileInputFormat;
import scut.se.mapred.completefile.NonSplittableTextInputFormat;
import scut.se.dbutils.HBaseOperator;
import scut.se.dbutils.HTableUntil;
import scut.se.dbutils.RowKeyGenerator;
import scut.se.entity.InvertedIndex;
import scut.se.entity.PageContent;
import scut.se.entity.PageInfo;
import scut.se.entity.PageJson;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InvertedIndicesGenerator {
    private static final String TABLE_SE = "searchengine";
    private static final String TABLE_HI = "htmlinfo";
    private static final HBaseOperator op = HBaseOperator.getInstance();

    public static class InvertedIndexMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {
        private Text filenameKey;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            filenameKey = new Text(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            String json = new String(value.copyBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
            Gson gson = new Gson();
            PageJson pageJson = gson.fromJson(json, PageJson.class);

            // 准备写入HBase
            PageInfo info = new PageInfo(pageJson.getUrl(), pageJson.getTitle(), pageJson.getFileName());
            PageContent content = new PageContent(pageJson.getHtml(), pageJson.getWords());
            // 生成rowKey
            String rowKey = RowKeyGenerator.getUUID();
            op.insertOneRowTo(TABLE_HI, info, rowKey);
            op.insertOneRowTo(TABLE_HI, content, rowKey);
            for (String word:
                 content.getWords()) {
                context.write(new Text(word), new Text(rowKey));
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /*Declare the Hash Map to store File name as key to compute and store number of times the filename is occurred for as value*/
            Map<String, Integer> m= new HashMap<>();
            int count = 0;
            for(Text t:values){
                String htmlNO = t.toString();
                /*Check if htmlNO is present in the HashMap*/
                if(m.get(htmlNO) != null){
                    count = m.get(htmlNO);
                    m.put(htmlNO, ++count);
                }else{
                    /*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
                    m.put(htmlNO, 1);
                }
            }

            StringBuilder htmlNOsInCSV = new StringBuilder();
            StringBuilder countsInCSV = new StringBuilder();
            for (Map.Entry<String, Integer> e: m.entrySet()) {
                htmlNOsInCSV.append(",");
                htmlNOsInCSV.append(e.getKey());
                countsInCSV.append(",");
                countsInCSV.append(e.getValue());
            }

            InvertedIndex invertedIndex = new InvertedIndex(key.toString(), htmlNOsInCSV.substring(1), countsInCSV.substring(1));
            String rowKey = RowKeyGenerator.getHash(key.toString());
            op.insertOneRowTo(TABLE_SE, invertedIndex, rowKey);
            /* Emit word and [file1→count of the word1 in file1 , file2→count of the word1 in file2 ………] as output*/
            context.write(key, new Text(m.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        // 查看是否已建表
        if (!HTableUntil.checkTableExist(TABLE_SE)) {
            op.createTable(TABLE_SE, Collections.singletonList(InvertedIndex.class.getName()));
        }
        if (!HTableUntil.checkTableExist(TABLE_HI)) {
            op.createTable(TABLE_HI, Arrays.asList(PageContent.class.getName(), PageInfo.class.getName()));
        }

        Configuration conf= new Configuration();
        Job job = new Job(conf,"InvertedIndexJob");
        //Defining the output key and value class for the mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setJarByClass(InvertedIndicesGenerator.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        //Defining the output value class for the reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(NonSplittableTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);

        CompleteFileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, outputPath);
        //deleting the output path automatically from hdfs so that we don't have delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);
        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
