package io.nubank.challenge.authorizer

import java.util.Properties

package object configs {

  def createStreamsProps: Properties = {
    val prop = new Properties()
    prop.put("TIME_WINDOW_SIZE_SECONDS", System.getenv("TIME_WINDOW_SIZE_SECONDS"))
    prop.put("TOPIC_QUEUE_SIZE", System.getenv("TOPIC_QUEUE_SIZE"))
    prop
  }

}
