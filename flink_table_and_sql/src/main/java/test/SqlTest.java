package test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


public class SqlTest {
    public static void main(String[] args) throws Exception {

        //设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple2<Long,String>> stream = env.readTextFile("D:\\BigData\\flink_projects\\flink-study\\flink_table_and_sql\\src\\main\\resources\\test.csv")
        .map(new Map());
//        stream.print();

        //流转换为表
        Table table1 = tableEnv.fromDataStream(stream,$("id"),$("address"));
        table1.printSchema();
        //转换为视图
        tableEnv.createTemporaryView("input",stream,$("id"),$("address"));
        //查询
        Table result = tableEnv.from("input").select($("id"), $("address"))
                .filter($("id").isEqual(2315));

        //Table不能直接打印
//        Table result = table1.select($("id"),$("address"));
        tableEnv.toAppendStream(result, new TupleTypeInfo<>(
                Types.LONG,
                Types.STRING
                )).print();

        Table result1 = tableEnv.sqlQuery("select id,address from input where id=10236");
        tableEnv.toAppendStream(result1,new TupleTypeInfo<>(
                Types.LONG,
                Types.STRING
        )).print();


//        tableEnv.connect(new FileSystem().path("D:\\BigData\\flink_projects\\flink-study\\flink_table_and_sql\\src\\main\\resources\\test.csv"))
//                .withFormat(new Csv().fieldDelimiter(',').deriveSchema())
//                .withSchema(new Schema()
//                .field("id", DataTypes.BIGINT())
//                        .field("uid",DataTypes.BIGINT())
//                        .field("address",DataTypes.STRING())
//                        .field("uaddress", DataTypes.STRING())
//                        .field("time",DataTypes.BIGINT())
//                )
//                .inAppendMode()
//                .createTemporaryTable("inputTable");
//
//        Table select = tableEnv.from("inputTable").select($("id"), $("address"));
//
//        select.printSchema();


//        tableEnv.execute("test job1");
        env.execute("test job");

    }
}


