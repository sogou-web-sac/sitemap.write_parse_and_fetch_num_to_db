/**
 * Created by zhufangze on 2017/6/2.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GetSitemapParseAndFetchNumReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        context.getCounter("user_reducer", "TOTAL").increment(1L);
        String root_sitemap = key.toString();

        Long n_parse = 0L;
        Long n_fetch = 0L;
        for (IntWritable i : values) {
            n_parse += 1;
            n_fetch += Integer.parseInt(i.toString() );
        }
        String res = n_parse.toString() + "\t" + n_fetch.toString();
        k.set(root_sitemap);
        v.set(res);
        context.write(k, v);
        context.getCounter("user_reducer", "OUTPUT").increment(1L);
    }
}
