package com.cn.me

import java.io.{FileNotFoundException, FileReader, IOException}

import sun.jvm.hotspot.HelloWorld

object HelloWorld {
  private var name: String = "";

  var age = 10;

  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    printtest()
    HelloWorld.age;
    var helloWorld1 = new HelloWorld();
    var helloWorld2 = new HelloWorld();
    println(helloWorld1 == helloWorld2)
    file()
  }

  def printtest() {
    println("ok");
  };

  def file() {
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException => {
        println("Missing file exception")
      }
      case ex: IOException => {
        println("IO Exception")
      }
    }
  }
}

