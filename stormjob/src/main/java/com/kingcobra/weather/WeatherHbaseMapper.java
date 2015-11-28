package com.kingcobra.weather;

import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/10/15.
 */
public class WeatherHbaseMapper implements HBaseMapper {
    private String key;
    private String columnF;
    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherHbaseMapper.class);
    public WeatherHbaseMapper(String key, String columnF) {
        this.key = key;
        this.columnF = columnF;
    }
    @Override
    public byte[] rowKey(Tuple tuple) {
        JSONObject data = (JSONObject) tuple.getValueByField("data");
        String[] a_key = key.split("\\.");
        StringBuilder sb = new StringBuilder();
        String k;
        for (int i=0;i<a_key.length;i++) {
            k = a_key[i];
            sb.append(data.get(k));
            if (i < a_key.length - 1) {
                sb.append(".");
            }
        }
        String rowKey = sb.toString();
        return Bytes.toBytes(rowKey);
    }

    @Override
    public ColumnList columns(Tuple tuple) {
        JSONObject data = (JSONObject) tuple.getValueByField("data");
        ColumnList columnList = new ColumnList();
        Set<Map.Entry<String, Object>> kvSet = data.entrySet();
        for (Map.Entry<String, Object> entry : kvSet) {
            LOGGER.debug("CF:{},CNAME:{},VALUE:{}", new Object[]{columnF, entry.getKey(), entry.getValue()});
            columnList.addColumn(Bytes.toBytes(columnF), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
        }
        return columnList;
    }
}
