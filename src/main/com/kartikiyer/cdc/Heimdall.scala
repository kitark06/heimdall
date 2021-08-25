
package com.kartikiyer.cdc

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.kartikiyer.cdc.model.RowEventData
import com.kartikiyer.cdc.serde.StringDeSerializer
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties

object Heimdall {

  def main(args: Array[String]): Unit = {

    println("kitark "+args.toSeq.mkString(","))

    val host = args{0}
    val port = args{1}.toInt
    val username = args{2}
    val password = args{3}
    val tableName = args{4}

    val props = new Properties()
      props.setProperty("database.allowPublicKeyRetrieval","true")
      props.setProperty("database.useSSL","false")
//      props.setProperty("event.deserialization.failure.handling.mode","warn")
//      props.setProperty("inconsistent.schema.handling.mode","warn")

    val sourceFunction = MySQLSource.builder[RowEventData]()
      .hostname(host)
      .port(port)
      .username(username)
      .password(password)
      .tableList(tableName)
      .debeziumProperties(props)
      .startupOptions(StartupOptions.initial())
      .deserializer(new StringDeSerializer()) // converts SourceRecord to String
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.addSource(sourceFunction)

    source
      .map(changeEvent => changeEvent.getJson) // do something with your data
      .print()

    env.execute()
  }


}