package phi

sealed trait Clock {
  def millis: Long
}

case object WallClock extends Clock {
  def millis: Long = System.currentTimeMillis
}

class TestClock(var time: Long) extends Clock {
  def millis = time
}
