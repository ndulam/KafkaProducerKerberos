package com.naresh.org.utils

import scala.collection.mutable.ArrayBuffer

/**
 * Handler for all external write systems
 * @author naresh
 */
trait Handler {
  /**
   * Closes the stream
   */
  def close()
}
