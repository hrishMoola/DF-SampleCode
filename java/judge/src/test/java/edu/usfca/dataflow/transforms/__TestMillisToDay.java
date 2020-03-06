package edu.usfca.dataflow.transforms;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class __TestMillisToDay {
  @Rule
  public Timeout timeout = Timeout.millis(2000);

  @Test
  public void testMillisToDay() {
    // Note that we "round down" any digits below the hour.
    // Try https://currentmillis.com/ or other websites to convert UNIX millis to/from utc datetime.
    assertEquals(18320L, ExtractData.ExtractAddicts.millisToDay(1582877633257L));
    assertEquals(18320L, ExtractData.ExtractAddicts.millisToDay(1582877700000L));
    assertEquals(18320L, ExtractData.ExtractAddicts.millisToDay(1582876800000L));
    assertEquals(18320L, ExtractData.ExtractAddicts.millisToDay(1582880400000L));

    // Feb 29 00:00:00AM in UTC
    assertEquals(18321L, ExtractData.ExtractAddicts.millisToDay(1582934400000L));
    // 1 millisecond before Feb 29 00:00:00AM in UTC
    assertEquals(18320L, ExtractData.ExtractAddicts.millisToDay(1582934400000L - 1L));
  }
}
