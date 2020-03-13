package edu.usfca.dataflow.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Event.PurchaseEvent;
import edu.usfca.protobuf.Event.PurchaseEvent.Store;

/**
 * Purpose & Context: See Lab 05 for more details.
 *
 */
public class LogParser {

  /**
   * Parsing rules (of the log messages) are mostly the same as in Lab 05.
   *
   * Here are some "new" rules/differences:
   *
   * (1) Now you should return KV &lt; DeviceId, PurchaseEvent &gt; (not just PurchaseEvent proto) where DeviceId should
   * be parsed based on the query URL (this is the new thing you need to incorporate).
   *
   * "uuid" will be given as either "ios_idfa" parameter (for iOS) or "gps_adid" (for android). If both parameters are
   * non-empty or both are empty, then consider the input log invalid. Also, note that these two fields are
   * case-insensitive, and thus you should normalize them here.
   *
   * (2) When input log is invalid, return null (instead of throwing an exception).
   *
   * ------------------------------------------------------------------------------
   *
   * You can copy your LogParser class from Lab05, and make necessary changes.
   *
   * OR, you can use the instructor's sample LogParser class (from reference solutions), and make necessary changes.
   *
   * Either way is acceptable, but you still need to ensure that your code passes all unit tests of this project.
   */
  public static KV<DeviceId, PurchaseEvent> getIdAndPurchaseEvent(String jsonLogAsLine) {
    try {
      PurchaseEvent.Builder pe = PurchaseEvent.newBuilder();
      JsonObject jsonLog = parser.parse(jsonLogAsLine).getAsJsonObject();

      // timestamp e.g. "timestamp": "2017-05-01T00:59:58.717127597Z"
      final String timestamp = jsonLog.get("timestamp").getAsString();
      try {
        pe.setEventAt(extractTimestampFromAccessLog(timestamp));
      } catch (Exception e) {
        return null;
      }

      final JsonObject httpReq = jsonLog.getAsJsonObject("httpRequest");
      final String reqUrlString = httpReq.get("requestUrl").getAsString();

      final int status = httpReq.has("status") ? httpReq.get("status").getAsInt() : -999;
      // OK
      if (status != 200) {
        return null;
      }

      Map<String, String> queryMap;
      try {
          queryMap = getQueryMap(reqUrlString);
        final DeviceId id = getDeviceId(queryMap);
        if (id == null) {
          return null;
        }
        final String bundle = queryMap.getOrDefault("bundle", "");
        if (StringUtils.isBlank(bundle)) {
          return null;
        }
        pe.setAppBundle(bundle);
        final String eventType = queryMap.getOrDefault("event_type", "");
        if (!PURCHASE_EVENTS.contains(eventType.toLowerCase())) {
          return null;
        }

        final String eventId = queryMap.getOrDefault("event_id", "");
        if (StringUtils.isBlank(eventId)) {
          return null;
        }
        pe.setEventId(eventId);
        final String store = queryMap.getOrDefault("store", "unknown_store").toUpperCase();
        try {
          pe.setStore(Store.valueOf(store));
        } catch (Exception e) {
          pe.setStore(Store.UNKNOWN_STORE);
        }

        final String amount = queryMap.getOrDefault("amount", null);
        if (amount == null) {
          return null;
        }
        final int amountInt = Integer.parseInt(String.format("%.0f", Double.parseDouble(amount)));
        pe.setAmount(amountInt);


        return KV.of(id, pe.build());
      } catch (URISyntaxException e) {
        // Terminate on URI syntax exception
        return null;
      }
    } catch (Exception eee) {
      return null;
    }
  }

  // NOTE: you should check if the code below is correct or not; make changes if necessary.

  private static DeviceId getDeviceId(Map<String, String> queryMap) {
    DeviceId.Builder did = DeviceId.newBuilder();

    final String adid = queryMap.getOrDefault("gps_adid", "");
    final String idfa = queryMap.getOrDefault("ios_idfa", "");
    if (StringUtils.isBlank(idfa) == StringUtils.isBlank(adid)) {
      return null;
    }

    if (!StringUtils.isBlank(idfa)) {
      did.setOs(OsType.IOS).setUuid(idfa.toUpperCase());
    } else {
      did.setOs(OsType.ANDROID).setUuid(adid.toUpperCase());
    }

    return did.build();
  }

  final static JsonParser parser = new JsonParser();
  final static Set<String> PURCHASE_EVENTS =
      new ImmutableSet.Builder<String>().add("purchase", "iap", "in-app-purchase").build();

  private static DateTimeFormatter DATE_TIME_NANOS_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").withZoneUTC();
  public static final DateTimeFormatter DATE_TIME_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

  // If timestamp string cannot be extracted or there is a parse error, this method will throw an exception.
  public static long extractTimestampFromAccessLog(String timestamp) {
    // e.g. "timestamp": "2017-04-28T10:59:58.907784163Z" or "2017-06-28T10:21:29Z"
    timestamp = timestamp.replace("T", " ").replace("Z", "");
    try {
      return DATE_TIME_NANOS_FORMAT.parseDateTime(timestamp).getMillis();
    } catch (IllegalArgumentException e) {
      return DATE_TIME_FORMAT.parseDateTime(timestamp).getMillis();
    }
  }

  public static Map<String, String> getQueryMap(String reqUrlString) throws URISyntaxException {
    // final String reqUrlStringEncoded = reqUrlString.replace("{", "%7B").replace("}", "%7D");

    return splitQuery(reqUrlString);
  }

  public static Map<String, String> splitQuery(String urlString) throws URISyntaxException {
    // While parsing, the encoded URL (hex-encodings, reserved characters, etc) is decoded
    final List<NameValuePair> parsedPairs = URLEncodedUtils.parse(new URI(urlString), Charset.forName("UTF-8"));
    Map<String, String> queryPairs = new LinkedHashMap<>();

    for (NameValuePair nv : parsedPairs) {
      queryPairs.put(nv.getName(), nv.getValue());
    }
    return queryPairs;
  }
}
