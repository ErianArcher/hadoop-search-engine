package scut.se.mapred;

import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import scut.se.dbutils.HBaseOperator;
import scut.se.dbutils.HTableUntil;
import scut.se.dbutils.RowKeyGenerator;
import scut.se.entity.InvertedIndex;
import scut.se.entity.PageContent;
import scut.se.entity.PageInfo;
import scut.se.entity.PageJson;
import scut.se.mapred.completefile.CombineCompleteFileInputFormat;
import scut.se.mapred.completefile.NonSplittableTextInputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.lang.reflect.Type;

import static scut.se.dbutils.TableNameEnum.TABLE_HI;
import static scut.se.dbutils.TableNameEnum.TABLE_SE;

public class InvertedIndicesGenerator {
    //private static final HBaseOperator op = HBaseOperator.getInstance();

    private static String isNullThenDefaultString(String target) {
        return target == null? "": target;
    }

    private static String safeSubString(StringBuilder sb, int start) {
        int length = sb.length();
        if (length <= start) return "";
        else return sb.substring(start);
    }

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        /*private Text filenameKey;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            filenameKey = new Text(path.toString());
        }*/

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Type listType = new TypeToken<ArrayList<PageJson>>(){}.getType();
            StringBuilder jsonsb = new StringBuilder();
            jsonsb.append("[");
            jsonsb.append(new String(value.copyBytes(), 0, value.getLength(), StandardCharsets.UTF_8));
            jsonsb.deleteCharAt(jsonsb.length()-1);
            jsonsb.append("]");
            Gson gson = new Gson();
            List<PageJson> pageJsonList = gson.fromJson(jsonsb.toString(), listType);

            // 准备写入HBase
            for (PageJson pageJson : pageJsonList) {
                if (!pageJson.getWords().isEmpty()) { // 首先判断是否为有效输入
                    PageInfo info = new PageInfo(isNullThenDefaultString(pageJson.getUrl()),
                            isNullThenDefaultString(pageJson.getTitle()),
                            isNullThenDefaultString(pageJson.getFilename()));
                    StringBuilder wordsInCSV = new StringBuilder();

                    for (String w :
                            pageJson.getWords()) {
                        wordsInCSV.append(",");
                        wordsInCSV.append(w.trim().replaceAll(",", ""));
                    }
                    PageContent content = new PageContent(isNullThenDefaultString(pageJson.getHtml()),
                            safeSubString(wordsInCSV, 1));
                    // 生成rowKey
                    String rowKey = RowKeyGenerator.getUUID();
                    //op.insertOneRowTo(TABLE_HI, info, rowKey);
                    //op.insertOneRowTo(TABLE_HI, content, rowKey);

                    // 优化1： 减少新建对象个数
                    Text key4Reducer = new Text();
                    Text value4Reducer = new Text();
                    for (String word :
                            content.getWords()) {
                        key4Reducer.set(word);
                        value4Reducer.set(rowKey);
                        context.write(key4Reducer, value4Reducer);
                    }
                }
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

            if (!m.entrySet().isEmpty()) {
                StringBuilder htmlNOsInCSV = new StringBuilder();
                StringBuilder countsInCSV = new StringBuilder();
                for (Map.Entry<String, Integer> e : m.entrySet()) {
                    htmlNOsInCSV.append(",");
                    htmlNOsInCSV.append(e.getKey());
                    countsInCSV.append(",");
                    countsInCSV.append(e.getValue());
                }

                InvertedIndex invertedIndex = new InvertedIndex(key.toString(), safeSubString(htmlNOsInCSV, 1),
                        safeSubString(countsInCSV, 1));
                String rowKey = RowKeyGenerator.getHash(key.toString());
                //op.insertOneRowTo(TABLE_SE, invertedIndex, rowKey);
                /* Emit word and [file1→count of the word1 in file1 , file2→count of the word1 in file2 ………] as output*/
                context.write(key, new Text(m.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 查看是否已建表
        /*if (!HTableUntil.checkTableExist(TABLE_SE)) {
            op.createTable(TABLE_SE, Collections.singletonList(InvertedIndex.class.getName()));
        }
        if (!HTableUntil.checkTableExist(TABLE_HI)) {
            op.createTable(TABLE_HI, Arrays.asList(PageContent.class.getName(), PageInfo.class.getName()));
        }*/

        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf,"InvertedIndexJob");
        //Defining the output key and value class for the mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setJarByClass(InvertedIndicesGenerator.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        //Defining the output value class for the reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CombineCompleteFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);

        CombineCompleteFileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, outputPath);
        //deleting the output path automatically from hdfs so that we don't have delete it explicitly
        outputPath.getFileSystem(conf).deleteOnExit(outputPath);
        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
