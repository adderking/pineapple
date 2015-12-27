package scistor.testspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by kingcobra on 15/8/20.
 */
public class SparkToHbase {
    private static final Configuration HBASECONF = HBaseConfiguration.create();
    private static final String OUTPUTPATH = "/user/kingcobra/hfile";
    private static final String INPUTPATH = "hdfs://hadoop4:9000/user/testData.txt";

    private static final String HBASETABLENAME = "test";
    private static final String CF = "cf1";
    static {
        HBASECONF.set("fs.defaultFS","hdfs://hadoop4:9000");
        HBASECONF.set("hbase.zookeeper.quorum", "hadoop4:2181,hadoop5:2181,hadoop6:2181");
        HBASECONF.set("hbase.rootdir","hdfs://hadoop4:9000/hbase");
    }
    public static void writeHFile(String path) throws Exception {
//        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("jsparkTest");
        SparkConf sparkConf = new SparkConf().setAppName("jsparkTest");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD = sparkContext.textFile(path);

        JavaPairRDD< String, String> s_pairRDD = javaRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] _a = s.split(",");
                String _key = _a[0] + "_" + _a[3] + "_" + _a[7];
                return new Tuple2<String, String>(_key, s);
            }
        }).sortByKey();
        JavaPairRDD<ImmutableBytesWritable,KeyValue> pairRDD = s_pairRDD.mapToPair(new PairFunction<Tuple2<String, String>, ImmutableBytesWritable, KeyValue>() {
            ImmutableBytesWritable rowKey;
            KeyValue kv;
           @Override
           public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<String, String> stringStringTuple2) throws Exception {
               rowKey = new ImmutableBytesWritable(stringStringTuple2._1().getBytes());
               kv = new KeyValue(stringStringTuple2._1().getBytes(), CF.getBytes(), "value".getBytes(), stringStringTuple2._2().getBytes());
               return new Tuple2<ImmutableBytesWritable, KeyValue>(rowKey, kv);
           }
       });
        Job job = Job.getInstance(HBASECONF);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HTable table = new HTable(HBASECONF, HBASETABLENAME);
        HFileOutputFormat2.configureIncrementalLoad(job, table);
        FileSystem fs = FileSystem.get(HBASECONF);
        fs.deleteOnExit(new Path(OUTPUTPATH));
//        fs.close();
        pairRDD.saveAsNewAPIHadoopFile(OUTPUTPATH,ImmutableBytesWritable.class,KeyValue.class,HFileOutputFormat2.class,HBASECONF);
//        sparkContext.stop();

    }
    public static void loadHFile(String path) throws Exception {
        HTable table = new HTable(HBASECONF, HBASETABLENAME);
        LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(HBASECONF);
        loadIncrementalHFiles.doBulkLoad(new Path(path),table);
    }

    public static void main(String[] args) {
        String path;
        try {
            if (args.length > 0) {
                path = args[0];
            }else{
                path = INPUTPATH;
            }
            SparkToHbase.writeHFile(path);
            SparkToHbase.loadHFile(OUTPUTPATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
