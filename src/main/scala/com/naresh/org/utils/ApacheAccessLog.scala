package com.naresh.org.utils

case class ApacheAccessLog(ipAddress: String, clientIdentd: String,
                           userId: String, dateTime: String, method: String,
                           requestURI: String, protocol: String,
                           responseCode: Int, contentSize: Long) {

}



object ApacheAccessLog {

  def toInt(in: String): Option[Int] = {
    try {
      Some(Integer.parseInt(in.trim))
    } catch {
      case e: NumberFormatException => None
    }
  }

  val PATTERN_COMMON = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|\-)""".r
  val PATTERN_COMBINED = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|\-) (\S+) (\S+)""".r



  def parseLogLine(log: String): Either[MalformedRecordException,ApacheAccessLog] = {
    val res = PATTERN_COMMON.findFirstMatchIn(log)
    if (res.isEmpty) {
      Left(new MalformedRecordException())
    }
    else {
      val m = res.get
      Right(ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
        m.group(5), m.group(6), m.group(7), m.group(8).toInt, {toInt(m.group(9)) match {
          case Some(i) => i.toLong
          case None => 0L
        }}))
    }
  }
}
