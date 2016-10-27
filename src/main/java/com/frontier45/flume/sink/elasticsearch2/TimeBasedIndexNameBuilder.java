package com.frontier45.flume.sink.elasticsearch2;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.formatter.output.BucketPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedIndexNameBuilder
  implements IndexNameBuilder
{
  private static final Logger logger = LoggerFactory.getLogger(TimeBasedIndexNameBuilder.class);
  public static final String DATE_FORMAT = "dateFormat";
  public static final String TIME_ZONE = "timeZone";
  public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
  public static final String DEFAULT_TIME_ZONE = "Etc/UTC";
  private Map<String, String> groupIdMap = new HashMap();
  private boolean InclusiveHost = true;

  private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("Etc/UTC"));
  private String indexPrefix;

  @VisibleForTesting
  FastDateFormat getFastDateFormat()
  {
    return this.fastDateFormat;
  }

  public String getIndexName(Event event)
  {
    TimestampedEvent timestampedEvent = new TimestampedEvent(event);
    long timestamp = timestampedEvent.getTimestamp();
    String realIndexPrefix = BucketPath.escapeString(this.indexPrefix, event.getHeaders());
    String group_id = "";
    Map headers = event.getHeaders();

    if (!this.groupIdMap.isEmpty()) {
      if ((headers.containsKey("interface")) || (headers.containsKey("src_ip")) || (headers.containsKey("dest_ip"))) {
        String src_group = (String)headers.get("interface") + "^" + (String)headers.get("src_ip");
        String dest_group = (String)headers.get("interface") + "^" + (String)headers.get("dest_ip");
        if (this.groupIdMap.containsKey(src_group))
          group_id = (String)this.groupIdMap.get(src_group);
        else if (this.groupIdMap.containsKey(dest_group))
          group_id = (String)this.groupIdMap.get(dest_group);
        else {
          group_id = "unknown";
        }
        group_id = realIndexPrefix + '-' + group_id + '-' + 
          this.fastDateFormat.format(timestamp);
      }
    }
    else group_id = realIndexPrefix + '-' + "unknown" + '-' + this.fastDateFormat.format(timestamp);

    return group_id;
  }

  public String getIndexPrefix(Event event)
  {
    return BucketPath.escapeString(this.indexPrefix, event.getHeaders());
  }

  public void configure(Context context)
  {
    String dateFormatString = context.getString("dateFormat");
    String timeZoneString = context.getString("timeZone");
    String groupIdPath = null;

    if (StringUtils.isBlank(dateFormatString)) {
      dateFormatString = "yyyy-MM-dd";
    }
    if (StringUtils.isBlank(timeZoneString)) {
      timeZoneString = "Etc/UTC";
    }

    groupIdPath = context.getString("groupIdPath", "/home/soft/flume/conf/groupId");
    this.InclusiveHost = context.getBoolean("InclusiveHost", Boolean.valueOf(true)).booleanValue();
    try
    {
      FileReader fileReader = new FileReader(groupIdPath);
      BufferedReader br = new BufferedReader(fileReader);
      String ipstr;
      while ((ipstr = br.readLine()) != null)
      {
        SubnetUtils utils = new SubnetUtils(ipstr.split("\\^")[2]);
        if (this.InclusiveHost)
          utils.setInclusiveHostCount(true);
        for (String ipaddr : utils.getInfo().getAllAddresses())
          this.groupIdMap.put(ipstr.split("\\^")[1] + "^" + ipaddr, ipstr.split("\\^")[3]);
      }
      br.close();
      fileReader.close();
    } catch (FileNotFoundException e) {
      return;
    } catch (IOException e) {
      return;
    }
    String ipstr;
    this.fastDateFormat = FastDateFormat.getInstance(dateFormatString, TimeZone.getTimeZone(timeZoneString));
    this.indexPrefix = context.getString("indexName");
  }

  public void configure(ComponentConfiguration conf)
  {
  }
}