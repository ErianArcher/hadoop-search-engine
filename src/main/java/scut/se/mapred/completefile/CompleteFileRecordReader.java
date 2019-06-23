package scut.se.mapred.completefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class CompleteFileRecordReader extends RecordReader<LongWritable, Text>{

    private FileSplit fileSplit;
    private Configuration conf;
    private Text value = new Text();
    private boolean processed = false;
    private long key = 0L;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
        Random random = new Random();
        key = random.nextLong();
    }
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(new String(contents, 0, value.getLength(), StandardCharsets.UTF_8));
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(this.key);
    }
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }
    @Override
    public void close() throws IOException {
        // do nothing
    }
}