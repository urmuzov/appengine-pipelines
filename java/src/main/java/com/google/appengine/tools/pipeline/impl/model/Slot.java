// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.Value;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A slot to be filled in with a value.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class Slot extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-slot";
  private static final String FILLED_PROPERTY = "filled";
  private static final String VALUE_PROPERTY = "value";
  private static final String WAITING_ON_ME_PROPERTY = "waitingOnMe";
  private static final String FILL_TIME_PROPERTY = "fillTime";
  private static final String SOURCE_JOB_KEY_PROPERTY = "sourceJob";

  // persistent
  private boolean filled;
  private Date fillTime;
  private Object value;
  private Key sourceJobKey;
  private final List<Key> waitingOnMeKeys;

  // transient
  private List<Barrier> waitingOnMeInflated;
  private Blob serializedVersion;

  public Slot(Key rootJobKey, Key generatorJobKey, String graphGUID) {
    super(rootJobKey, generatorJobKey, graphGUID);
    waitingOnMeKeys = new LinkedList<>();
  }

  public Slot(Entity entity) {
    this(entity, false);
  }

  public Slot(Entity entity, boolean lazy) {
    super(entity);
    filled = entity.getBoolean(FILLED_PROPERTY);
    if (entity.getNames().contains(FILL_TIME_PROPERTY)) {
      fillTime = new Date(entity.getTimestamp(FILL_TIME_PROPERTY).getSeconds() * 1000);
    }
    if (entity.getNames().contains(SOURCE_JOB_KEY_PROPERTY)) {
      sourceJobKey = entity.getKey(SOURCE_JOB_KEY_PROPERTY);
    }
    waitingOnMeKeys = getListProperty(WAITING_ON_ME_PROPERTY, entity);
    if (lazy) {
      serializedVersion = entity.getBlob(VALUE_PROPERTY);
    } else {

      value = deserializeValue(entity.getValue(VALUE_PROPERTY));
    }
  }

  private Object deserializeValue(Value serializedValue) {
    try {
      return PipelineManager.getBackEnd().deserializeValue(this, serializedValue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Entity toEntity() {
    Entity.Builder entity = toProtoEntity();
    entity.set(FILLED_PROPERTY, filled);
    if (null != fillTime) {
      entity.set(FILL_TIME_PROPERTY, Timestamp.of(fillTime));
    }
    if (null != sourceJobKey) {
      entity.set(SOURCE_JOB_KEY_PROPERTY, sourceJobKey);
    }
    entity.set(WAITING_ON_ME_PROPERTY, KeyHelper.convertKeyList(waitingOnMeKeys));
    if (serializedVersion != null) {
      entity.set(VALUE_PROPERTY, serializedVersion);
    } else {
      try {
        Value serialized =
            PipelineManager.getBackEnd().serializeValue(this, value);
        if (serialized instanceof BlobValue || serialized instanceof ListValue) {
          entity.set(VALUE_PROPERTY, serialized);
        } else {
          throw new RuntimeException("PipelineManager.getBackEnd().serializeValue return unknown");
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return entity.build();
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public void inflate(Map<Key, Barrier> pool) {
    waitingOnMeInflated = buildInflated(waitingOnMeKeys, pool);
  }

  public void addWaiter(Barrier waiter) {
    waitingOnMeKeys.add(waiter.getKey());
    if (null == waitingOnMeInflated) {
      waitingOnMeInflated = new LinkedList<>();
    }
    waitingOnMeInflated.add(waiter);
  }

  public boolean isFilled() {
    return filled;
  }

  public Object getValue() {
    if (serializedVersion != null) {
      value = deserializeValue(BlobValue.of(serializedVersion));
      serializedVersion = null;
    }
    return value;
  }

  /**
   * Will return {@code null} if this slot is not filled.
   */
  public Date getFillTime() {
    return fillTime;
  }

  public Key getSourceJobKey() {
    return sourceJobKey;
  }

  public void setSourceJobKey(Key key) {
    sourceJobKey = key;
  }

  public void fill(Object value) {
    filled = true;
    this.value = value;
    serializedVersion = null;
    fillTime = new Date();
  }

  public List<Key> getWaitingOnMeKeys() {
    return waitingOnMeKeys;
  }

  /**
   * If this slot has not yet been inflated this method returns null;
   */
  public List<Barrier> getWaitingOnMeInflated() {
    return waitingOnMeInflated;
  }

  @Override
  public String toString() {
    return "Slot[" + getKeyName(getKey()) + ", value=" + (serializedVersion != null ? "..." : value)
        + ", filled=" + filled + ", waitingOnMe=" + waitingOnMeKeys + ", parent="
        + getKeyName(getGeneratorJobKey()) + ", guid=" + getGraphGuid() + "]";
  }
}
