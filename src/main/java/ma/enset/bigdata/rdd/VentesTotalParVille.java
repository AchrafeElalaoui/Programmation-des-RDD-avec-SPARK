package ma.enset.bigdata.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class VentesTotalParVille {
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: VentesTotalParVille <ventes.txt>");
      System.exit(1);
    }

    SparkConf conf = new SparkConf().setAppName("TP1-Ex1-TotalVentesParVille");
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]");
    }

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      JavaRDD<String> lines = sc.textFile(args[0]).filter(s -> !s.trim().isEmpty());

      JavaPairRDD<String, Double> ventesParVille =
          lines.mapToPair(line -> {
                String[] parts = line.trim().split("\\s+");
                String ville = parts[1];
                double prix = Double.parseDouble(parts[3]);
                return new Tuple2<>(ville, prix);
              })
              .reduceByKey(Double::sum);

      ventesParVille
          .sortByKey(true)
          .collect()
          .forEach(t -> System.out.println(t._1() + "\t" + t._2()));
    }
  }
}

