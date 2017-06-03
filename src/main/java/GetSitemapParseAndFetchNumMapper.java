/**
 * Created by zhufangze on 2017/6/2.
 */
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GetSitemapParseAndFetchNumMapper
        extends TableMapper<Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable();

    public void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {

        context.getCounter("user_mapper", "TOTAL").increment(1L);

        try {
            String root_sitemap = new String( value.getValue(Bytes.toBytes("i"), Bytes.toBytes("top_smap")) );
            Long fetch_ts     = Long.parseLong(new String( value.getValue(Bytes.toBytes("i"), Bytes.toBytes("fetch_ts"))));

            if (fetch_ts == 0) {
                v.set(0);
            }else {
                v.set(1);
            }

            k.set(root_sitemap);
            context.write(k, v);
            context.getCounter("user_mapper", "OUTPUT").increment(1L);
        } catch(Exception e) {
            context.getCounter("user_mapper", e.toString()).increment(1L);
        }
    }

}
