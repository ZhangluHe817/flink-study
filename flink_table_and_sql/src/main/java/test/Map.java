package test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Map extends RichMapFunction<String, Tuple2<Long,String>> {


    @Override
    public Tuple2<Long,String> map(String s) throws Exception {
        String[] split = s.split(",");
        Tuple2<Long, String> tu = new Tuple2<Long, String>();
        tu.f0 = new Long(split[0]);
        tu.f1 = split[3];
        return tu;
    }
}
