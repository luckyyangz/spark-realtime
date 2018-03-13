
package com.udgrp.common

import java.sql.{Connection => MysqlConnection}
import java.util.Properties

import com.udgrp.conf.Constants
import com.udgrp.pools.base.PoolConfig
import com.udgrp.pools.jdbc.JdbcConnectionPool
import com.udgrp.pools.kafka.KafkaConnectionPool
import com.udgrp.pools.zk.ZKConnectionPool
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.Producer
import org.slf4j.LoggerFactory

/**
  * Created by kejw on 2017/5/23.
  */
trait ConnPools extends Serializable {
  @transient val logger = LoggerFactory.getLogger(classOf[ConnPools])
  private var jdbcConnectionPool: JdbcConnectionPool = _
  private var kafkaConnectionPool: KafkaConnectionPool = _
  private var zkConnectionPool: ZKConnectionPool = _

  private def getPoolConfig: PoolConfig = {
    val poolConfig: PoolConfig = new PoolConfig

    val maxidle = Constants.MAXIDLE
    val maxtotal = Constants.MAXTOTAL
    val maxwaitmillis = Constants.MAXWAITMILLIS
    val testonborrow = Constants.TESTONBORROW
    val testonreturn = Constants.TESTONRETURN
    val testoncreate = Constants.TESTONCREATE

    poolConfig.setMaxIdle(maxidle)
    poolConfig.setMaxTotal(maxtotal)
    poolConfig.setMaxWaitMillis(maxwaitmillis)
    poolConfig.setTestOnBorrow(testonborrow)
    poolConfig.setTestOnReturn(testonreturn)
    poolConfig.setTestOnCreate(testoncreate)
    poolConfig
  }

  private def initJdbcPool: JdbcConnectionPool = {
    logger.info("init jdbc ...")
    val driver = Constants.DRIVER
    val userName = Constants.USERNAME
    val password = Constants.PASSWORD
    val url = Constants.URL
    val jdbcPool = new JdbcConnectionPool(getPoolConfig, driver, url, userName, password)
    logger.info("init jdbc sucdessfully")
    jdbcPool
  }


  def getJdbcConn: MysqlConnection = {
    synchronized {
      if (jdbcConnectionPool == null || jdbcConnectionPool.isClosed) {
        jdbcConnectionPool = initJdbcPool
      }
    }
    jdbcConnectionPool.getConnection
  }

  def returnjdbcConn(conn: MysqlConnection): Unit = {
    synchronized {
      jdbcConnectionPool.returnConnection(conn)
    }
  }

  private def initKafka: KafkaConnectionPool = {
    val kafkaConfig = new Properties()
    logger.info("init kafka ...")
    kafkaConfig.setProperty("bootstrap.servers", Constants.KAFKA_SERVER)
    kafkaConfig.setProperty("acks", Constants.ACKS)
    kafkaConfig.setProperty("retries", Constants.RETRIES)
    kafkaConfig.setProperty("batch.size", Constants.BATCH_SIZE)
    kafkaConfig.setProperty("linger.ms", Constants.LINGER_MS)
    kafkaConfig.setProperty("reconnect.backoff.ms", Constants.RECONNECT_BACKOFF_MS)
    kafkaConfig.setProperty("max.in.flight.requests.per.connection", Constants.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
    kafkaConfig.setProperty("retry.backoff.ms", Constants.RETRY_BACKOFF_MS)
    kafkaConfig.setProperty("compression.type", Constants.COMPRESSION_TYPE)
    kafkaConfig.setProperty("buffer.memory", Constants.BUFFER_MEMORY)
    kafkaConfig.setProperty("key.serializer", Constants.KEY_SERIALIZER)
    kafkaConfig.setProperty("value.serializer", Constants.VALUE_SERIALIZER)
    kafkaConfig.setProperty("partitioner.class", Constants.PARTITION_CLASS)
    val kafkaPool = new KafkaConnectionPool(getPoolConfig, kafkaConfig)
    logger.info("init kafka sucdessfully")
    kafkaPool
  }

  private def initZK: ZKConnectionPool = {
    logger.info("init zookeeper ...")
    val zkServers = Constants.ZK_CLUSTER
    val zkPool = new ZKConnectionPool(zkServers)
    logger.info("init zookeeper sucdessfully")
    zkPool
  }

  def getKafkaConn: Producer[String, String] = {
    synchronized {
      if (kafkaConnectionPool == null || kafkaConnectionPool.isClosed) {
        kafkaConnectionPool = initKafka
      }
    }
    kafkaConnectionPool.getConnection
  }

  def returnKafkaConn(producer: Producer[String, String]): Unit = {
    synchronized {
      kafkaConnectionPool.returnConnection(producer)
    }
  }

  def getZKConn: ZkUtils = {
    synchronized {
      if (zkConnectionPool == null || zkConnectionPool.isClosed) {
        zkConnectionPool = initZK
      }
    }
    zkConnectionPool.getConnection
  }

  def returnZKConn(zkUtils: ZkUtils): Unit = {
    synchronized {
      zkConnectionPool.returnConnection(zkUtils)
    }
  }
}
