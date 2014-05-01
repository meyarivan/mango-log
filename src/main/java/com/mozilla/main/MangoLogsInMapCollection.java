package com.mozilla.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Vector;
import java.util.EnumMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ua_parser.Client;
import ua_parser.Parser;
import ua_parser.CachingParser;

import com.maxmind.geoip.LookupService;
import com.mozilla.custom.parse.LogLine;
import com.mozilla.domain.CheckLogLine;
import com.mozilla.geo.IPtoGeo;
import com.mozilla.lib.CombineFileTextInputFormat;
import com.mozilla.lib.LogLineInfo;

public class MangoLogsInMapCollection extends Configured implements Tool {
  /**
   * The map class of WordCount.
   */
    
  static enum LOG_PROGRESS { REDUCER_COUNT, REDUCER_COUNT_IO, REDUCER_COUNT_IE, ZERO_SIZED_HTTP_REQUEST, MAPPER_LINE_COUNT, INVALID_ANONYMOUS_SPLIT_COUNT, ERROR_UA_PARSER, LOG_LINES, INVALID_SPLIT, VALID_SPLIT, ERROR_DISTRIBUTED_CACHE, ERROR_GEOIP_DAT_URI_MISSING, ERROR_GEOIP_CITY_DAT_URI_MISSING, ERROR_GEOIP_DOMAIN_DAT_URI_MISSING, SETUP_CALLS, ERROR_GEOIP_LOOKUP,ERROR_REGEXES_YAML_LOOKUP, INVALID_DATE_FORMAT, INVALID_GEO_LOOKUP, VALID_ANONYMOUS_LINE_COUNT, INVALID_ANONYMOUS_LINE_COUNT, VALID_RAW_LINE_COUNT, ERROR_GEOIP_ISP_DAT_URI_MISSING, ERROR_GEOIP_ORG_DAT_URI_MISSING, DEBUG_COUNTER };

  public static String ANONYMIZED_PREFIX = "anonymized";
  public static String RAW_PREFIX = "raw";
  public static String ERROR_PREFIX = "error";
  public static String DISTRIBUTED_CACHE_URI = "/user/metrics-etl/maxmind/";
  public static String GEOIP_CITY_DAT = "GeoIPCity.dat";
  public static String GEOIP_ORG_DAT = "GeoIPOrg.dat";
  public static String GEOIP_DOMAIN_DAT = "GeoIPDomain.dat";
  public static String GEOIP_ISP_DAT = "GeoIPISP.dat";
  public static String DOMAIN_NAME = "DOMAIN_NAME";

  public static class MangoLogsInMapCollectionMapper extends Mapper<Object, LogLineInfo, Text, Text> {
    //public static final Log LOG =  LogFactory.getLog("ReadLzoFile"); 

    private final Text anonymizedPrefixKey = new Text(ANONYMIZED_PREFIX);
    private final Text rawPrefixKey = new Text(RAW_PREFIX);
    private final Text textKey = new Text();
    private final Text textVal = new Text();

    private String input_fname, domain_name;
    private String[] splitSlash;
    private FileSplit fileSplit;
    private CheckLogLine cll;

    private MultipleOutputs<Text, Text> mos;
    private Path[] localFiles;
    private LookupService cityLookup, domainLookup, orgLookup, ispLookup;
    private boolean validAnonymizedLine = true;
    private StringBuffer sb;
    private IPtoGeo iptg;
    private boolean missingDatFile = false;
    private Parser ua_parser;
    private Client c_parser;
    private InputStream is;
    private LogLine logline;
    private EnumMap<LOG_PROGRESS, Long> counters = new EnumMap<LOG_PROGRESS, Long>(LOG_PROGRESS.class);

    /**
     * runs before starting every mapper
     * useful to get the file split name from the mapper
     */



    public void setup(Context context) {
      cll = new CheckLogLine();
      mos = new MultipleOutputs<Text, Text>(context);
      incrCounter(LOG_PROGRESS.SETUP_CALLS, 1);

      try {
        localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        domain_name = context.getConfiguration().get(DOMAIN_NAME);
      } catch (IOException e) {
        incrCounter(LOG_PROGRESS.ERROR_DISTRIBUTED_CACHE, 1);
        e.printStackTrace();
      }

      if (localFiles.length > 0) {
        for (Path localFile : localFiles) {
          if ((localFile.getName() != null) && (localFile.getName().equalsIgnoreCase(GEOIP_CITY_DAT))) {
            try {
              cityLookup = new LookupService(new File(localFile.toUri().getPath()), LookupService.GEOIP_MEMORY_CACHE);
            } catch (IOException e) {
              missingDatFile = true;
              incrCounter(LOG_PROGRESS.ERROR_GEOIP_LOOKUP, 1);
            }
          } 

          else if ((localFile.getName() != null) && (localFile.getName().equalsIgnoreCase(GEOIP_DOMAIN_DAT))) {
            try {
              domainLookup = new LookupService(new File(localFile.toUri().getPath()), LookupService.GEOIP_MEMORY_CACHE);
            } catch (IOException e) {
              missingDatFile = true;
              incrCounter(LOG_PROGRESS.ERROR_GEOIP_LOOKUP, 1);
            }
          } 

          else if ((localFile.getName() != null) && (localFile.getName().equalsIgnoreCase(GEOIP_ORG_DAT))) {
            try {
              orgLookup = new LookupService(new File(localFile.toUri().getPath()), LookupService.GEOIP_MEMORY_CACHE);
            } catch (IOException e) {
              missingDatFile = true;
              incrCounter(LOG_PROGRESS.ERROR_GEOIP_LOOKUP, 1);
            }
          } 

          else if ((localFile.getName() != null) && (localFile.getName().equalsIgnoreCase(GEOIP_ISP_DAT))) {
            try {
              ispLookup = new LookupService(new File(localFile.toUri().getPath()), LookupService.GEOIP_MEMORY_CACHE);
            } catch (IOException e) {
              missingDatFile = true;
              incrCounter(LOG_PROGRESS.ERROR_GEOIP_LOOKUP, 1);
            }
          } 

          else if ((localFile.getName() != null) && (localFile.getName().equalsIgnoreCase("regexes.yaml"))) {
            try {
              is = new FileInputStream(new File(localFile.toUri().getPath()));
              ua_parser = new CachingParser(is);
            } catch (IOException e) {
              missingDatFile = true;
              incrCounter(LOG_PROGRESS.ERROR_REGEXES_YAML_LOOKUP, 1);
            }
          } 
        }

        if (missingDatFile) {
          incrCounter(LOG_PROGRESS.ERROR_GEOIP_DAT_URI_MISSING, 1);
        }
      }




    }

    private void incrCounter(LOG_PROGRESS counter, int val) {
      long v = counters.containsKey(counter) ? counters.get(counter) : 0;
      counters.put(counter, v + val);
    }
        
    public void map(Object key, LogLineInfo logLine, Context context) throws IOException, InterruptedException {
      Text value = logLine.getLogLine();
      input_fname = logLine.getFileName().toString();

      incrCounter(LOG_PROGRESS.MAPPER_LINE_COUNT, 1);

      if (value.find("\"  \" - 0 \"-\" \"-\" \"-\"") != -1) {
        incrCounter(LOG_PROGRESS.ZERO_SIZED_HTTP_REQUEST, 1);
        return;
      } 

      String v = cll.logLine(domain_name, value.toString());
      validAnonymizedLine = true;
      try {
        logline = new LogLine(v, domain_name);
        if (StringUtils.equals(domain_name, "marketplace.mozilla.org")) {
          incrCounter(LOG_PROGRESS.DEBUG_COUNTER, 1);
        }
        if (logline.getSplitCount() > 0) {
          incrCounter(LOG_PROGRESS.VALID_SPLIT, 1);
          textVal.set(logline.getRawTableString());
          context.write(rawPrefixKey, textVal);

          mos.write(RAW_PREFIX, rawPrefixKey, textVal);

          incrCounter(LOG_PROGRESS.VALID_RAW_LINE_COUNT, 1);

          if (logline.addDate()) {
            if (!logline.addGeoLookUp(cityLookup, domainLookup, ispLookup, orgLookup)) {
              validAnonymizedLine = false;
            }
          } else {
            //TODO: add error date counter
            validAnonymizedLine = false;
          }
          logline.addHttpLogInfo();

          if (!logline.addUserAgentInfo(ua_parser)) {
            validAnonymizedLine = false;
          } 

          logline.addCustomAndOtherInfo();

          if (!logline.addFilename(input_fname)) { //value.toString()
            validAnonymizedLine = false;
          }

          if (validAnonymizedLine) {
            textVal.set(logline.getOutputLine());

            if (logline.checkOutputFormat()) {
              mos.write(ANONYMIZED_PREFIX, anonymizedPrefixKey, textVal);
              incrCounter(LOG_PROGRESS.VALID_ANONYMOUS_LINE_COUNT, 1);
            } else {
              validAnonymizedLine = false;
              incrCounter(LOG_PROGRESS.INVALID_ANONYMOUS_SPLIT_COUNT, 1);
            }

          } else {
            incrCounter(LOG_PROGRESS.INVALID_ANONYMOUS_LINE_COUNT, 1);

          }

        } else {
          validAnonymizedLine = false;
          if (v.contains("\"  \" - 0 \"-\" \"-\" \"-\"")) {
            incrCounter(LOG_PROGRESS.ZERO_SIZED_HTTP_REQUEST, 1);
          } else {
            incrCounter(LOG_PROGRESS.INVALID_SPLIT, 1);
          }
        }

      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } finally {
        mos.write(ERROR_PREFIX, new Text(ERROR_PREFIX), new Text(value));
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (LOG_PROGRESS x : counters.keySet())
        context.getCounter(x).increment(counters.get(x));

      mos.close();
    }
  }

  public static String getJobDate(String input) {
    // /user/aphadke/tmp/temp_intermediate_raw_anon_logs-addons.mozilla.org-2013-06-03/
    String[] splitSlash = StringUtils.split(input, "/");
    if (splitSlash.length > 0) {
      String[] splitDash = StringUtils.split(splitSlash[3],"-");
      return (splitDash[2] + "-" + splitDash[3] + "-" + splitDash[4]);
    }


    return "";
  }
  /**
   * The main entry point.
   */
  public int run(String[] args) throws Exception {
    String inputPath, outputPath, domainName, logDate;

    if (args.length < 4) {
      System.err.println("Usage: input output domain-name date");
      return 1;
    }

    inputPath = args[0];
    outputPath = args[1];
    domainName = args[2];
    logDate = args[3];

    Configuration c = getConf();
    c.set(DOMAIN_NAME, domainName);

    DistributedCache.addCacheFile(new URI(DISTRIBUTED_CACHE_URI + GEOIP_CITY_DAT), c);
    DistributedCache.addCacheFile(new URI(DISTRIBUTED_CACHE_URI + GEOIP_DOMAIN_DAT), c);
    DistributedCache.addCacheFile(new URI(DISTRIBUTED_CACHE_URI + GEOIP_ISP_DAT), c);
    DistributedCache.addCacheFile(new URI(DISTRIBUTED_CACHE_URI + GEOIP_ORG_DAT), c);
    DistributedCache.addCacheFile(new URI(DISTRIBUTED_CACHE_URI + "regexes.yaml"), c);

    Job job;
    job = new Job(c);
    job.setJobName("Logs: " + domainName + " : "  + logDate);

    job.setJarByClass(MangoLogsInMapCollection.class);
    job.setMapperClass(MangoLogsInMapCollectionMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setInputFormatClass(CombineFileTextInputFormat.class);        
        
    FileInputFormat.addInputPath(job, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

    MultipleOutputs.addNamedOutput(job, ANONYMIZED_PREFIX, SequenceFileOutputFormat.class , Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, RAW_PREFIX, SequenceFileOutputFormat.class , Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, ERROR_PREFIX, SequenceFileOutputFormat.class , Text.class, Text.class);
    
    job.setNumReduceTasks(0);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MangoLogsInMapCollection(), args);
         
    System.exit(res);
  }
}






