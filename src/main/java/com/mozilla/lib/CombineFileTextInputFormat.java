package com.mozilla.lib;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class CombineFileTextInputFormat extends CombineFileInputFormat<LongWritable,Text> {
  public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<LongWritable,Text>((CombineFileSplit)split, context, LineRecordReaderWrapper.class);
  }
  
  private static class LineRecordReaderWrapper extends LineRecordReader {
    Integer splitId;

    public LineRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
      splitId = idx;
    }

    @Override
    public void initialize(InputSplit fSplit,
                           TaskAttemptContext context) throws IOException {

      CombineFileSplit inputSplit = (CombineFileSplit) fSplit;
      super.initialize(new FileSplit(inputSplit.getPath(splitId), inputSplit.getOffset(splitId), inputSplit.getLength(splitId), inputSplit.getLocations()), context);
    }
  }
}