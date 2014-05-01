package com.mozilla.lib;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogLineInfo implements Writable {
  private Text logLine, fileName;

  public LogLineInfo() {
    logLine = new Text();
    fileName = new Text();
  }
  
  public LogLineInfo(Text logL, Text fileN) {
    this.set(logL, fileN);
  }

  public void write(DataOutput dataOutput) throws IOException {
    logLine.write(dataOutput);
    fileName.write(dataOutput);
  }

  public void readFields(DataInput dataInput) throws IOException {
    logLine.readFields(dataInput);
    fileName.readFields(dataInput);
  }

  public Text getLogLine() {
    return logLine;
  }

  public Text getFileName() {
    return fileName;
  }

  public void set(Text logL, Text fileN) {
    logLine = logL;
    fileName = fileN;
  }    
}