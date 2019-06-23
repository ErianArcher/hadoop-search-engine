package scut.se.mapred.completefile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class CombineCompleteFileInputFormat extends CombineFileInputFormat<LongWritable, Text> {

    private static final long SPLITSIZE = 134217728; // 128 MB in bytes

    public CombineCompleteFileInputFormat() {
        super();
        setMaxSplitSize(SPLITSIZE);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit) split, context, MultiFileRecordReader.class);
    }

}