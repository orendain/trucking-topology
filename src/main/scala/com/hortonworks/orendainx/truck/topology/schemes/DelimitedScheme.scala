package com.hortonworks.orendainx.truck.topology

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.storm.spout.Scheme
import org.apache.storm.utils.{Utils => StormUtils}

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
abstract class DelimitedScheme(delimiter: String) extends Scheme {

  /**
    *
    * @param string
    * @return
    */
  def deserializeString(string: ByteBuffer): Array[String] = {
    // TODO: match if cleaner
    val rawString = if (string.hasArray) {
      val base = string.arrayOffset()
      new String(string.array(), base + string.position(), string.remaining())
    } else {
      val x = StormUtils.toByteArray(string)
      new String(x, UTF_8)
    }

    rawString.split(delimiter)
  }
}
