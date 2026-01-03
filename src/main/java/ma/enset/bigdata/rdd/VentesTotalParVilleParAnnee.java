package ma.enset.bigdata.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class VentesTotalParVilleParAnnee {
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: VentesTotalParVilleParAnnee <ventes.txt>");
      System.exit(1);
    }

    SparkConf conf = new SparkConf().setAppName("TP1-Ex1-TotalVentesParVilleParAnnee");
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]");
    }

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      JavaRDD<String> lines = sc.textFile(args[0]).filter(s -> !s.trim().isEmpty());

      JavaPairRDD<Tuple2<String, Integer>, Double> ventes =
          lines.mapToPair(line -> {
                String[] parts = line.trim().split("\\s+");
                String date = parts[0]; // yyyy-mm-dd
                int annee = Integer.parseInt(date.substring(0, 4));
                String ville = parts[1];
                double prix = Double.parseDouble(parts[3]);
                return new Tuple2<>(new Tuple2<>(ville, annee), prix);
              })
              .reduceByKey(Double::sum);

      ventes
          .mapToPair(kv -> new Tuple2<>(kv._1()._1() + "\t" + kv._1()._2(), kv._2()))
          .sortByKey(true)
          .collect()
          .forEach(t -> System.out.println(t._1() + "\t" + t._2()));
    }
  }
}

