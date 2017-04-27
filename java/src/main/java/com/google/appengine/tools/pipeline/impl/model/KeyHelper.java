package com.google.appengine.tools.pipeline.impl.model;

import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyValue;
import com.google.cloud.datastore.LongValue;
import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;

import java.util.List;

public class KeyHelper {
  private static com.google.cloud.datastore.KeyFactory factory;

  private synchronized static com.google.cloud.datastore.KeyFactory getFactory() {
    if (factory == null) {
      factory = DatastoreOptions.getDefaultInstance().getService().newKeyFactory();
    }
    return factory;
  }

  public static final Key createKey(final String kind, final String name) {
    return getFactory().setKind(kind).newKey(name);
  }

  public static Key stringToKey(final String in) {
    int modulo = in.length() % 4;
    final String urlSafe;
    if (modulo != 0) {
      urlSafe = in.concat("====".substring(modulo));
    } else {
      urlSafe = in;
    }
    byte[] decodedBytes = BaseEncoding.base64Url().decode(CharMatcher.whitespace().removeFrom(urlSafe));
    return Key.fromUrlSafe(new String(decodedBytes));
  }

  public static String keyToString(final Key key) {
    return BaseEncoding.base64Url().omitPadding().encode(key.toUrlSafe().getBytes());
  }

  public static Entity.Builder setKeyList(final Entity.Builder builder, final String propertyName, final List<Key> keys) {
    if (keys.size() == 1) {
      builder.set(propertyName, keys.get(0));
    } else if (keys.size() == 2) {
      builder.set(propertyName, keys.get(0), keys.get(1));
    } else if (keys.size() > 2) {
      builder.set(propertyName, keys.get(0), keys.get(1), keys.subList(2, keys.size()).toArray(new Key[keys.size() - 2]));
    }
    return builder;
  }

  public static List<KeyValue> convertKeyList(final List<Key> keys) {
    List<KeyValue> out = Lists.newArrayList();
    for (Key key : keys) {
      out.add(new KeyValue(key));
    }
    return out;
  }

  public static List<LongValue> convertLongList(final List<Long> longs) {
    List<LongValue> out = Lists.newArrayList();
    for (Long l : longs) {
      out.add(new LongValue(l));
    }
    return out;
  }
}
