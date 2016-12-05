package com.aronia.core

/**
  * Main portal for ads application
  */
object AroniaServer {

  def main(args: Array[String]): Unit = {
    val context = new AroniaContext
    context.go
  }
}
