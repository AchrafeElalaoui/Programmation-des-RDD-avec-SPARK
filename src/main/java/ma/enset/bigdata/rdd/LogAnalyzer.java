package ma.enset.bigdata.rdd;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class LogAnalyzer {
  // Basic Apache combined log parser: ip ... [date] "METHOD URL PROTO" status size "referer" "ua"
  private static final Pattern LOG_PATTERN = Pattern.compile(
      "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)]\\s+\"(\\S+)\\s+(\\S+)\\s+(\\S+)\"\\s+(\\d{3})\\s+(\\S+).*$"
  );

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: LogAnalyzer <access.log>");
      System.exit(1);
    }

    SparkConf conf = new SparkConf().setAppName("TP1-Ex2-LogAnalyzer");
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]");
    }

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      JavaRDD<String> lines = sc.textFile(args[0]).filter(s -> !s.trim().isEmpty());

      JavaRDD<LogEntry> entries = lines
          .map(LogAnalyzer::parse)
          .filter(e -> e != null)
          .cache();

      long total = entries.count();
      long errors = entries.filter(e -> e.getStatus() >= 400).count();
      double errorPct = total == 0 ? 0.0 : (errors * 100.0) / total;

      System.out.println("Total requêtes: " + total);
      System.out.println("Total erreurs (>=400): " + errors);
      System.out.println("Pourcentage erreurs: " + String.format("%.2f", errorPct) + "%");

      JavaPairRDD<String, Integer> ipCounts =
          entries.mapToPair(e -> new Tuple2<>(e.getIp(), 1)).reduceByKey(Integer::sum);
      List<Tuple2<String, Integer>> topIps = ipCounts.takeOrdered(5, new DescCountThenKey());
      System.out.println("\nTop 5 IP:");
      topIps.forEach(t -> System.out.println(t._1() + "\t" + t._2()));

      JavaPairRDD<String, Integer> resCounts =
          entries.mapToPair(e -> new Tuple2<>(e.getResource(), 1)).reduceByKey(Integer::sum);
      List<Tuple2<String, Integer>> topRes = resCounts.takeOrdered(5, new DescCountThenKey());
      System.out.println("\nTop 5 ressources:");
      topRes.forEach(t -> System.out.println(t._1() + "\t" + t._2()));

      JavaPairRDD<Integer, Integer> codeCounts =
          entries.mapToPair(e -> new Tuple2<>(e.getStatus(), 1)).reduceByKey(Integer::sum);
      System.out.println("\nRépartition par code HTTP:");
      codeCounts
          .sortByKey(true)
          .collect()
          .forEach(t -> System.out.println(t._1() + "\t" + t._2()));
    }
  }

  private static LogEntry parse(String line) {
    Matcher m = LOG_PATTERN.matcher(line);
    if (!m.matches()) {
      return null;
    }
    String ip = m.group(1);
    String dateTime = m.group(2);
    String method = m.group(3);
    String resource = m.group(4);
    int status = Integer.parseInt(m.group(6));
    String sizeRaw = m.group(7);
    long size = "-".equals(sizeRaw) ? 0L : Long.parseLong(sizeRaw);
    return new LogEntry(ip, dateTime, method, resource, status, size);
  }

  private static final class DescCountThenKey
      implements Comparator<Tuple2<String, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
      int byCount = Integer.compare(b._2(), a._2());
      if (byCount != 0) return byCount;
      return a._1().compareTo(b._1());
    }
  }
}

