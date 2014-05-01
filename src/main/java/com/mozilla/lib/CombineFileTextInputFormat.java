package com.mozilla.lib;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class CombineFileTextInputFormat extends CombineFileInputFormat<LongWritable,LogLineInfo> {
  public RecordReader<LongWritable,LogLineInfo> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<LongWritable,LogLineInfo>((CombineFileSplit)split, context, LineRecordReaderWrapper.class);
  }
  
  private static class LineRecordReaderWrapper extends RecordReader<LongWritable, LogLineInfo> {
    Integer splitId;
    Text splitPath;
    LineRecordReader lineReader;
    LogLineInfo value = new LogLineInfo();

    public LineRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
      splitId = idx;
    }
    
    @Override
    public void initialize(InputSplit fSplit,
                           TaskAttemptContext context) throws IOException {

      CombineFileSplit inputSplit = (CombineFileSplit) fSplit;
      splitPath = new Text(inputSplit.getPath(splitId).getName());

      lineReader = new LineRecordReader();
      lineReader.initialize(new FileSplit(inputSplit.getPath(splitId), inputSplit.getOffset(splitId), inputSplit.getLength(splitId), inputSplit.getLocations()), context);
    }
    
    public boolean nextKeyValue() throws IOException {
      return lineReader.nextKeyValue();
    }
    
    public LogLineInfo getCurrentValue() {
      value.set(lineReader.getCurrentValue(), splitPath);
      return value;
    }

    public LongWritable getCurrentKey() {
      return lineReader.getCurrentKey();
    }

    public void close() throws IOException {
      lineReader.close();
    }

    public float getProgress() {
      return lineReader.getProgress();
    }
  }
}