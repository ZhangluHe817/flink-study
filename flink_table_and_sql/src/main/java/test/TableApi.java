package test;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class TableApi {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //连接文件
        tableEnv.connect(new FileSystem()
                .path("D:\\BigData\\flink_projects\\flink-study\\flink_table_and_sql\\src\\main\\resources\\test.csv"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.BIGINT())
                        .field("uid",DataTypes.BIGINT())
                        .field("address",DataTypes.STRING())
                        .field("uaddress", DataTypes.STRING())
                        .field("time",DataTypes.BIGINT()))
                .createTemporaryTable("input");

        Table table = tableEnv.from("input").groupBy($("id")).select($("id").count().as("count"),$("id"));
        tableEnv.toRetractStream(table,new TupleTypeInfo<>(
                Types.LONG,
//                Types.LONG,
//                Types.STRING,
//                Types.STRING,
                Types.LONG
        )).print();

        //连接kafka
        tableEnv.connect( new Kafka()
        .version("0.11")
        .topic("test")
        .property("zookeeper.connect","localhost:2181")
        .property("bootstrap.servers","localhost:9092"))
        .withFormat(new Csv())
                .withSchema( new Schema()
                .field("id", DataTypes.BIGINT()))
                .createTemporaryTable("kafka_input");



        tableEnv.connect(new FileSystem()
                .path("D:\\BigData\\flink_projects\\flink-study\\flink_table_and_sql\\src\\main\\resources\\out.csv"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.BIGINT())
                        .field("uid",DataTypes.BIGINT())
                        .field("address",DataTypes.STRING())
                        .field("uaddress", DataTypes.STRING())
                        .field("time",DataTypes.BIGINT()))
                .createTemporaryTable("out");

        Table table1 = tableEnv.sqlQuery("select * from input");

//        table.executeInsert("out");

//        tableEnv.execute("job");
        env.execute("job");
    }
}
