package com.aronia.core

import java.sql.{Connection, DriverManager}

import com.typesafe.scalalogging.LazyLogging
import com.aronia.core.AroniaUtils._
import com.aronia.core.spiders.GithubStargazers
import us.codecraft.webmagic.Spider

import scala.collection.mutable.ArrayBuffer

/**
  * Global context
  */
private[aronia] class AroniaContext extends LazyLogging {

  var spiders = new ArrayBuffer[Spider]
  val config = new AroniaConfig(true)
  private val defaultGlobalConfig = "ads_env.conf"

  // read environment variables from external file
  val gitConfig = {
    val configFile = config.get(config.ADS_ENV_CONFIG, null)
    if (configFile == null) {
      logger.info(s"Using default ads configurations ($defaultGlobalConfig)")
      loadKeyValueResource(defaultGlobalConfig)
    } else {
      logger.info(s"Using user-specified configuration file ($configFile)")
      loadKeyValueFile(configFile)
    }
  }

  config.setAll(gitConfig)
  // TODO: determine the priority of jvm and config file

  // global mysql backend
  classOf[com.mysql.jdbc.Driver]
  private val host = config.get(config.MYSQL_HOST, "127.0.0.1")
  private val port = config.get(config.MYSQL_PORT, "3306")
  private val username = config.get(config.MYSQL_USERNAME, "root")
  private val passwd = config.get(config.MYSQL_PASSWORD, "password")
  private val jdbcAddr = s"jdbc:mysql://$host:$port?user=$username&password=$passwd"

  logger.info(s"Connecting to $jdbcAddr")
  val mysqlBackend: Connection = DriverManager.getConnection(jdbcAddr)

  // configure all spiders
  private val githubStarSpider = (new GithubStargazers)
    .createSpider(this, Array(config.GITHUB_AWESOME_REPO), 10)
  spiders += githubStarSpider

  def go(): Unit = {
    logger.info("Running all spiders")
    spiders.foreach(_.run)
  }

  def clear(): Unit = {
    if (mysqlBackend != null)
      mysqlBackend.close()
  }

}
