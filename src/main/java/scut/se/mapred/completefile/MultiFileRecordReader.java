package scut.se.mapred.completefile;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MultiFileRecordReader extends RecordReader<LongWritable, Text> {
    private CombineFileSplit split;
    private TaskAttemptContext context;
    private int index;
    private RecordReader<LongWritable, Text> recordReader;

    /**
     * This method is a constructor to initialize split context index and
     * SingleFileRecordReader.
     *
     * @param split
     * @param context
     * @param index
     */
    public MultiFileRecordReader(CombineFileSplit split,
                                 TaskAttemptContext context, Integer index) {
        this.split = split;
        this.context = context;
        this.index = index;
        recordReader = new CompleteFileRecordReader();
    }

    @Override
    /**
     * This method reads each filepath and initializes SingleFileRecordReader.
     */
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.split = (CombineFileSplit) split;
        this.context = context;

        if (null == recordReader) {
            recordReader = new CompleteFileRecordReader();
        }

        FileSplit fileSplit = new FileSplit(this.split.getPath(index),
                this.split.getOffset(index), this.split.getLength(index),
                this.split.getLocations());
        this.recordReader.initialize(fileSplit, this.context);

    }

    @Override
    /**
     * This method passes the value of next record.
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return this.recordReader.nextKeyValue();
    }


    @Override
    /**
     * This method returns the current key of the file value.
     */
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return this.recordReader.getCurrentKey();
    }

    @Override
    /**
     * This method returns the value of the current record.
     */
    public Text getCurrentValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return this.recordReader.getCurrentValue();
    }

    @Override
    /**
     * This method returns the progress of file split.
     */
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return this.recordReader.getProgress();
    }

    @Override
    /**
     * This method closes the record reader.
     */
    public void close() throws IOException {
        if (recordReader != null) {
            recordReader.close();
            recordReader = null;
        }
    }
}
