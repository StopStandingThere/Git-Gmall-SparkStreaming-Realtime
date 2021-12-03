package com.szl.utils

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

object PropertiesUtil {

  def load(propertyName: String): Properties = {
    val prop= new Properties()

    prop.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertyName),StandardCharsets.UTF_8))

    prop
  }
}
