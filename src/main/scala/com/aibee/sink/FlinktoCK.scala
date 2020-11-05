package com.aibee.sink


import java.sql.PreparedStatement
import java.text.SimpleDateFormat

import com.aibee.testdata.{SensorReading, SensorSource}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


class CkSinkBuilder1 extends JdbcStatementBuilder[(String,Int, String)] {
  def accept(ps: PreparedStatement, v: (String,Int, String)): Unit = {
    ps.setString(1, v._1)
    ps.setInt(2, v._2)
    ps.setString(3, v._3)
  }
}

object FlinktoCK {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      val value: DataStream[SensorReading] = env.addSource(new SensorSource)
      val stream = value.map(x => {
        val tem = x.temperature * 1000
        (x.id, tem.toInt, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(x.timestamp))
        // (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(x.timestamp),x.temperature)
      })
      stream.print("测试数据")
      //jdbc test01
      // stream.addSink(new MyJDBCSink)
      //jdbs test02
/*      val sql="insert into test.t_temperature(id,temperature,create_time)values(?,?,?)"
      stream.addSink(JdbcSink.sink[(String,Int, String)](sql,new CkSinkBuilder,new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:clickhouse://172.16.244.123:8123")
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUsername("default")
          .withPassword("123456")
          .build()
      ))*/
      env.execute()
    }
  }
}
