package com.kartikiyer.cdc

import org.apache.flink.client.deployment.StandaloneClusterId
import org.apache.flink.client.program.rest.RestClusterClient
import org.apache.flink.client.program.{PackagedProgram, PackagedProgramUtils}
import org.apache.flink.configuration.{Configuration, RestOptions}

import java.io.File

object Trigger {
  def main(args: Array[String]): Unit = {
    val PARALLELISM = 1
    val conf = new Configuration

    conf.setString("jobmanager.rpc.address", "localhost")
    conf.setInteger("jobmanager.rpc.port", 6123)
    conf.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 1)

    val flinkClient = new RestClusterClient[StandaloneClusterId](conf, StandaloneClusterId.getInstance)

    val program = PackagedProgram.newBuilder
      .setJarFile(new File("""R:\heimdall-fat.jar"""))
      .setEntryPointClassName("com.kartikiyer.cdc.Heimdall")
      .setArguments("192.168.0.194", "3306", "kartik","root","employees.dept_emp")
      //      .setSavepointRestoreSettings(SavepointRestoreSettings.forPath("/tmp-flink-savepoints/savepoint-88a4f6-a67c70ad9005"))
      .build()

    val jobGraph = PackagedProgramUtils.createJobGraph(program, conf, PARALLELISM, false)

    val jobId = flinkClient.submitJob(jobGraph).get()
    println(jobId)
  }
}
