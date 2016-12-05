package com.aronia.core

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
  * Created by chenxm on 7/19/16.
  */
class AroniaConfig(loadDefaults: Boolean) extends Cloneable {

  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    // Load any ads.* system properties
    for ((k, v) <- System.getProperties.asScala if k.startsWith("ads.")) {
      set(k, v)
    }
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): AroniaConfig = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings.put(key, value)
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings.putAll(settings.toMap.asJava)
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): AroniaConfig = {
    settings.putIfAbsent(key, value)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): AroniaConfig = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.containsKey(key)

  /** Copy this object */
  override def clone: AroniaConfig = {
    new AroniaConfig(false).setAll(getAll)
  }

  /** Constant values */
  val GITHUB_AWESOME_REPO = "caesar0301/awesome-public-datasets"

  /** Configuration keys */
  val ADS_ENV_CONFIG = "ads.env.conf"

  val GITHUB_CLIENT_KEY = "ads.github.client.id"
  val GITHUB_CLIENT_SCR = "ads.github.client.secret"
  val GITHUB_CLIENT_PER_PAGE = "ads.github.entries.per.page"

  val MYSQL_HOST = "ads.mysql.host"
  val MYSQL_PORT = "ads.mysql.port"
  val MYSQL_USERNAME = "ads.mysql.username"
  val MYSQL_PASSWORD = "ads.mysql.password"
  val MYSQL_DATABASE = "ads.mysql.database"
}