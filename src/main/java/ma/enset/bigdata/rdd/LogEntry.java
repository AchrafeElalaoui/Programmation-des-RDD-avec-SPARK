package ma.enset.bigdata.rdd;

import java.io.Serializable;

public final class LogEntry implements Serializable {
  private final String ip;
  private final String dateTimeRaw;
  private final String method;
  private final String resource;
  private final int status;
  private final long size;

  public LogEntry(String ip, String dateTimeRaw, String method, String resource, int status, long size) {
    this.ip = ip;
    this.dateTimeRaw = dateTimeRaw;
    this.method = method;
    this.resource = resource;
    this.status = status;
    this.size = size;
  }

  public String getIp() {
    return ip;
  }

  public String getDateTimeRaw() {
    return dateTimeRaw;
  }

  public String getMethod() {
    return method;
  }

  public String getResource() {
    return resource;
  }

  public int getStatus() {
    return status;
  }

  public long getSize() {
    return size;
  }
}

