package com.even.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * des: TableMapper是HBase提供的api
 * ImmutableBytesWritable类似之前的LongWritable，表示起始偏移量。put表示封装的一条一条数据，需要导入到Reduce阶段
 * author: Even
 * create date:2019/1/30
 */
public class HBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        /*1，读取数据，拿到一个rowkey的数据，根据key得到put*/
        Put put = new Put(key.get());
        /*2，遍历出每个列并筛选需要的*/
        for (Cell cell : value.rawCells()) {
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            if ("info".equals(cf)) {
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell))))
                    /*获取info列族的name列的数据，并导入到另一张表的info1列族的id列*/
                    put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("id"), CellUtil.cloneValue(cell));
            }
        }
        context.write(key, put);
    }
}
