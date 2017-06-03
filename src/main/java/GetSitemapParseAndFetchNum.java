/**
 * Created by zhufangze on 2017/6/2.
 * @param: void
 * @output: root_sitemap\tn_parse\tn_fetch
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetSitemapParseAndFetchNum extends Configured implements Tool {

    // to print config param
    public static void print_conf(Configuration conf) {
        try {
            conf.writeXml(System.out);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] arg0) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, conf.get("mapred.job.name"));
        String input_table = conf.get("hbase.table");


        job.setJarByClass(GetSitemapParseAndFetchNum.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("top_smap"));
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("fetch_ts"));

        TableMapReduceUtil.initTableMapperJob(
                input_table,
                scan,
                GetSitemapParseAndFetchNumMapper.class,
                Text.class,
                IntWritable.class,
                job);

        job.setReducerClass(GetSitemapParseAndFetchNumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(200); // this is need by hand code

        if (job.waitForCompletion(true) && job.isSuccessful()) {
            return 0;
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        print_conf(conf);

        int res = ToolRunner.run(conf, new GetSitemapParseAndFetchNum(), args);
        System.exit(res);
    }

}