package com.aronia.core

import java.io.{FileInputStream, InputStream}
import java.sql.{Connection, ResultSet, SQLException}

import us.codecraft.webmagic.selector.Json

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object AroniaUtils {

  /** Read kv configurations from file path */
  def loadKeyValueFile(file: String):  Array[(String, String)] = {
    val stream = new FileInputStream(file)
    loadKeyValueConfig(stream)
  }

  /** Read kv configurations from resource file */
  def loadKeyValueResource(resource: String): Array[(String, String)] = {
    val resourceClean = if(!resource.startsWith("/")) "/"+resource else resource
    val stream: InputStream = getClass.getResourceAsStream(resourceClean)
    if(stream == null)
      throw new NoSuchFieldError("Resource file not found, %s".format(resource))
    loadKeyValueConfig(stream)
  }

  /** Read kv configurations from input stream */
  def loadKeyValueConfig(stream: InputStream): Array[(String, String)] = {
    val lines = Source.fromInputStream(stream).getLines
    var settings = new ArrayBuffer[(String, String)]
    lines.foreach {line => {
      if (line.length > 0 && line.contains("=")) {
        val parts = line.split("=")
        settings += Tuple2(stripEnds(parts(0), " "), stripEnds(parts(1), " "))
      }
    }}
    settings.toArray
  }

  def stripEnds(in: String, ch: String): String = {
    in.stripPrefix(ch).stripSuffix(ch)
  }

  /**
    * Concatenate Url and parameters separated by question mark "?"
    */
  def assembleUrl(url: String, parameters: Map[String, String]): Option[String] = {
    if (url == null || url.length == 0)
      return None
    val paramStr = if (parameters.size > 0)
      "?" + parameters.map{case(key, value) => key+"="+value}.mkString("&")
    else
      ""
    Some(url + paramStr)
  }

  def trimUrlParams(url: String): String = {
    val cleanUrl = if(!url.contains("?")) url
      else url.split("\\?")(0)
    if (!cleanUrl.endsWith("/"))
      cleanUrl
    else
      cleanUrl.substring(0, cleanUrl.length-1)
  }

  def getJsonField(jsonPath: String, json: Json): Option[String] = {
    try {
      val value = json.jsonPath(jsonPath).get
      if (value == null) throw new Exception
      else Some(value)
    } catch {
      case any: Exception => None
    }
  }

  /**
    * @return true when the repo name is valid in form of "username/reponame"
    */
  def isLegalGithubRepo(repo: String): Boolean = {
    val pattern = "^[\\w\\-]+/[\\w\\-]+$".r
    val matched = pattern.findFirstMatchIn(repo)
    if (matched.orNull == null)
      false
    else
      true
  }

  /** Module test */
  def main(args: Array[String]): Unit = {
    assert(stripEnds(" aaaa ", " ") == "aaaa")
    assert(trimUrlParams("http://www.baidu.com/?A=aaaa")=="http://www.baidu.com")
    assert(isLegalGithubRepo("caesar0301/abc"))
  }
}
