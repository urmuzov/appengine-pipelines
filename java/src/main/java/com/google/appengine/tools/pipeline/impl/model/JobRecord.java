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

import com.google.appengine.api.backends.BackendService;
import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.IntValuedSetting;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;
import com.google.appengine.tools.pipeline.JobSetting.OnBackend;
import com.google.appengine.tools.pipeline.JobSetting.OnQueue;
import com.google.appengine.tools.pipeline.JobSetting.OnService;
import com.google.appengine.tools.pipeline.JobSetting.OnVersion;
import com.google.appengine.tools.pipeline.JobSetting.QueueRetryMaxBackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.QueueRetryMaxDoublings;
import com.google.appengine.tools.pipeline.JobSetting.QueueRetryMinBackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.QueueRetryTaskAgeLimitSeconds;
import com.google.appengine.tools.pipeline.JobSetting.QueueRetryTaskRetryLimit;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.JobSetting.WaitForSetting;
import com.google.appengine.tools.pipeline.impl.FutureValueImpl;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The Pipeline model object corresponding to a job.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class JobRecord extends PipelineModelObject implements JobInfo {

  /**
   * The state of the job.
   */
  public static enum State {
    // TODO(user): document states (including valid transitions) and relation to JobInfo.State
    WAITING_TO_RUN, WAITING_TO_FINALIZE, FINALIZED, STOPPED, CANCELED, RETRY
  }

  public static final String EXCEPTION_HANDLER_METHOD_NAME = "handleException";

  /**
   * This enum serves as an input parameter to the method
   * {@link com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#queryJob(
   *Key, InflationType)}. When fetching an
   * instance of {@code JobRecord} from the data store this enum specifies how
   * much auxiliary data should also be queried and used to inflate the instance
   * of {@code JobRecord}.
   *
   */
  public static enum InflationType {
    /**
     * Do not inflate at all
     */
    NONE,

    /**
     * Inflate as necessary to run the job. In particular:
     * <ul>
     * <li>{@link JobRecord#getRunBarrierInflated()} will not return
     * {@code null}; and
     * <li>for the returned {@link Barrier}
     * {@link Barrier#getWaitingOnInflated()} will not return {@code null}; and
     * <li> {@link JobRecord#getOutputSlotInflated()} will not return
     * {@code null}; and
     * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
     * {@code null}
     * </ul>
     */
    FOR_RUN,

    /**
     * Inflate as necessary to finalize the job. In particular:
     * <ul>
     * <li> {@link JobRecord#getOutputSlotInflated()} will not return
     * {@code null}; and
     * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
     * {@code null}; and
     * <li>for the returned {@link Barrier} the method
     * {@link Barrier#getWaitingOnInflated()} will not return {@code null}.
     * </ul>
     */
    FOR_FINALIZE,

    /**
     * Inflate as necessary to retrieve the output of the job. In particular
     * {@link JobRecord#getOutputSlotInflated()} will not return {@code null}
     */
    FOR_OUTPUT;
  }

  public static final String DATA_STORE_KIND = "pipeline-job";
  // Data store entity property names
  private static final String JOB_INSTANCE_PROPERTY = "jobInstance";
  private static final String RUN_BARRIER_PROPERTY = "runBarrier";
  private static final String FINALIZE_BARRIER_PROPERTY = "finalizeBarrier";
  public static final String STATE_PROPERTY = "state";
  private static final String EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY =
      "exceptionHandlingAncestorKey";
  private static final String EXCEPTION_HANDLER_SPECIFIED_PROPERTY = "hasExceptionHandler";
  private static final String EXCEPTION_HANDLER_JOB_KEY_PROPERTY = "exceptionHandlerJobKey";
  private static final String EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY =
      "exceptionHandlerJobGraphGuid";
  private static final String CALL_EXCEPTION_HANDLER_PROPERTY = "callExceptionHandler";
  private static final String IGNORE_EXCEPTION_PROPERTY = "ignoreException";
  private static final String OUTPUT_SLOT_PROPERTY = "outputSlot";
  private static final String EXCEPTION_KEY_PROPERTY = "exceptionKey";
  public static final String START_TIME_PROPERTY = "startTime";
  private static final String END_TIME_PROPERTY = "endTime";
  private static final String CHILD_KEYS_PROPERTY = "childKeys";
  private static final String ATTEMPT_NUM_PROPERTY = "attemptNum";
  private static final String MAX_ATTEMPTS_PROPERTY = "maxAttempts";
  private static final String BACKOFF_SECONDS_PROPERTY = "backoffSeconds";
  private static final String BACKOFF_FACTOR_PROPERTY = "backoffFactor";
  private static final String ON_BACKEND_PROPERTY = "onBackend";
  private static final String ON_SERVICE_PROPERTY = "onService";
  private static final String ON_VERSION_PROPERTY = "onVersion";
  private static final String ON_QUEUE_PROPERTY = "onQueue";
  private static final String QUEUE_RETRY_TASK_RETRY_LIMIT_PROPERTY = "queueRetryTaskRetryLimit";
  private static final String QUEUE_RETRY_TASK_AGE_LIMIT_SECONDS_PROPERTY = "queueRetryTaskAgeLimitSeconds";
  private static final String QUEUE_RETRY_TASK_MIN_BACKOFF_SECONDS_PROPERTY = "queueRetryMinBackoffSeconds";
  private static final String QUEUE_RETRY_TASK_MAX_BACKOFF_SECONDS_PROPERTY = "queueRetryMaxBackoffSeconds";
  private static final String QUEUE_RETRY_TASK_MAX_DOUBLINGS_PROPERTY = "queueRetryMaxDoublings";
  private static final String CHILD_GRAPH_GUID_PROPERTY = "childGraphGuid";
  private static final String STATUS_CONSOLE_URL = "statusConsoleUrl";
  public static final String ROOT_JOB_DISPLAY_NAME = "rootJobDisplayName";

  // persistent fields
  private final Key jobInstanceKey;
  private final Key runBarrierKey;
  private final Key finalizeBarrierKey;
  private Key outputSlotKey;
  private State state;
  private Key exceptionHandlingAncestorKey;
  private boolean exceptionHandlerSpecified;
  private Key exceptionHandlerJobKey;
  private String exceptionHandlerJobGraphGuid;
  private boolean callExceptionHandler;
  private boolean ignoreException;
  private Key exceptionKey;
  private Date startTime;
  private Date endTime;
  private String childGraphGuid;
  private List<Key> childKeys;
  private long attemptNumber;
  private long maxAttempts = JobSetting.MaxAttempts.DEFAULT;
  private long backoffSeconds = JobSetting.BackoffSeconds.DEFAULT;
  private long backoffFactor = JobSetting.BackoffFactor.DEFAULT;
  private final QueueSettings queueSettings = new QueueSettings();
  private String statusConsoleUrl;
  private String rootJobDisplayName;

  // transient fields
  private Barrier runBarrierInflated;
  private Barrier finalizeBarrierInflated;
  private Slot outputSlotInflated;
  private JobInstanceRecord jobInstanceRecordInflated;
  private Throwable exceptionInflated;

  /**
   * Re-constitutes an instance of this class from a Data Store entity.
   *
   * @param entity
   */
  public JobRecord(Entity entity) {
    super(entity);
    jobInstanceKey = entity.getKey(JOB_INSTANCE_PROPERTY);
    finalizeBarrierKey = entity.getKey(FINALIZE_BARRIER_PROPERTY);
    runBarrierKey = entity.getKey(RUN_BARRIER_PROPERTY);
    outputSlotKey = entity.getKey(OUTPUT_SLOT_PROPERTY);
    state = State.valueOf(entity.getString(STATE_PROPERTY));
    if (entity.getNames().contains(EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY)) {
      exceptionHandlingAncestorKey = entity.getKey(EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY);
    }
    if (entity.getNames().contains(EXCEPTION_HANDLER_SPECIFIED_PROPERTY)) {
      exceptionHandlerSpecified = entity.getBoolean(EXCEPTION_HANDLER_SPECIFIED_PROPERTY);
    }
    if (entity.getNames().contains(EXCEPTION_HANDLER_JOB_KEY_PROPERTY)) {
      exceptionHandlerJobKey = entity.getKey(EXCEPTION_HANDLER_JOB_KEY_PROPERTY);
    }
    if (entity.getNames().contains(EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY)) {
      exceptionHandlerJobGraphGuid = entity.getString(EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY);
    }
    callExceptionHandler = entity.getBoolean(CALL_EXCEPTION_HANDLER_PROPERTY);
    ignoreException = entity.getBoolean(IGNORE_EXCEPTION_PROPERTY);
    if (entity.getNames().contains(CHILD_GRAPH_GUID_PROPERTY)) {
      childGraphGuid = entity.getString(CHILD_GRAPH_GUID_PROPERTY);
    }
    if (entity.getNames().contains(EXCEPTION_KEY_PROPERTY)) {
      exceptionKey = entity.getKey(EXCEPTION_KEY_PROPERTY);
    }
    if (entity.getNames().contains(START_TIME_PROPERTY)) {
      final Timestamp dateTime = entity.getTimestamp(START_TIME_PROPERTY);
      startTime = dateTime != null ? new Date(dateTime.getSeconds() * 1000) : null;
    }
    if (entity.getNames().contains(END_TIME_PROPERTY)) {
      final Timestamp dateTime = entity.getTimestamp(END_TIME_PROPERTY);
      endTime = dateTime != null ? new Date(dateTime.getSeconds() * 1000) : null;
    }

    childKeys = getListProperty(CHILD_KEYS_PROPERTY, entity);

    attemptNumber = entity.getLong(ATTEMPT_NUM_PROPERTY);
    maxAttempts = entity.getLong(MAX_ATTEMPTS_PROPERTY);
    backoffSeconds = entity.getLong(BACKOFF_SECONDS_PROPERTY);
    backoffFactor = entity.getLong(BACKOFF_FACTOR_PROPERTY);
    if (entity.getNames().contains(ON_BACKEND_PROPERTY)) {
      queueSettings.setOnBackend(entity.getString(ON_BACKEND_PROPERTY));
    }
    if (entity.getNames().contains(ON_SERVICE_PROPERTY)) {
      queueSettings.setOnService(entity.getString(ON_SERVICE_PROPERTY));
    }
    if (entity.getNames().contains(ON_VERSION_PROPERTY)) {
      queueSettings.setOnVersion(entity.getString(ON_VERSION_PROPERTY));
    }
    if (entity.getNames().contains(ON_QUEUE_PROPERTY)) {
      queueSettings.setOnQueue(entity.getString(ON_QUEUE_PROPERTY));
    }
    if (entity.getNames().contains(QUEUE_RETRY_TASK_RETRY_LIMIT_PROPERTY)) {
      queueSettings.setQueueRetryTaskRetryLimit(entity.getLong(QUEUE_RETRY_TASK_RETRY_LIMIT_PROPERTY));
    }
    if (entity.getNames().contains(QUEUE_RETRY_TASK_AGE_LIMIT_SECONDS_PROPERTY)) {
      queueSettings.setQueueRetryTaskAgeLimitSeconds(entity.getLong(QUEUE_RETRY_TASK_AGE_LIMIT_SECONDS_PROPERTY));
    }
    if (entity.getNames().contains(QUEUE_RETRY_TASK_MIN_BACKOFF_SECONDS_PROPERTY)) {
      queueSettings.setQueueRetryMinBackoffSeconds(entity.getLong(QUEUE_RETRY_TASK_MIN_BACKOFF_SECONDS_PROPERTY));
    }
    if (entity.getNames().contains(QUEUE_RETRY_TASK_MAX_BACKOFF_SECONDS_PROPERTY)) {
      queueSettings.setQueueRetryMaxBackoffSeconds(entity.getLong(QUEUE_RETRY_TASK_MAX_BACKOFF_SECONDS_PROPERTY));
    }
    if (entity.getNames().contains(QUEUE_RETRY_TASK_MAX_DOUBLINGS_PROPERTY)) {
      queueSettings.setQueueRetryMaxDoublings(entity.getLong(QUEUE_RETRY_TASK_MAX_DOUBLINGS_PROPERTY));
    }
    if (entity.getNames().contains(STATUS_CONSOLE_URL)) {
      statusConsoleUrl = entity.getString(STATUS_CONSOLE_URL);
    }
    if (entity.getNames().contains(ROOT_JOB_DISPLAY_NAME)) {
      rootJobDisplayName = entity.getString(ROOT_JOB_DISPLAY_NAME);
    }
  }

  /**
   * Constructs and returns a Data Store Entity that represents this model
   * object
   */
  @Override
  public Entity toEntity() {
    Entity.Builder entity = toProtoEntity();
    entity.set(JOB_INSTANCE_PROPERTY, jobInstanceKey);
    entity.set(FINALIZE_BARRIER_PROPERTY, finalizeBarrierKey);
    entity.set(RUN_BARRIER_PROPERTY, runBarrierKey);
    entity.set(OUTPUT_SLOT_PROPERTY, outputSlotKey);
    entity.set(STATE_PROPERTY, state.toString());
    if (null != exceptionHandlingAncestorKey) {
      entity.set(EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY, exceptionHandlingAncestorKey);
    }
    if (exceptionHandlerSpecified) {
      entity.set(EXCEPTION_HANDLER_SPECIFIED_PROPERTY, Boolean.TRUE);
    }
    if (null != exceptionHandlerJobKey) {
      entity.set(EXCEPTION_HANDLER_JOB_KEY_PROPERTY, exceptionHandlerJobKey);
    }
    if (null != exceptionKey) {
      entity.set(EXCEPTION_KEY_PROPERTY, exceptionKey);
    }
    if (null != exceptionHandlerJobGraphGuid) {
      entity.set(
          EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY, exceptionHandlerJobGraphGuid);
    }
    entity.set(CALL_EXCEPTION_HANDLER_PROPERTY, callExceptionHandler);
    entity.set(IGNORE_EXCEPTION_PROPERTY, ignoreException);
    if (childGraphGuid != null) {
      entity.set(CHILD_GRAPH_GUID_PROPERTY, childGraphGuid);
    }
    if (startTime != null) {
      entity.set(START_TIME_PROPERTY, Timestamp.of(startTime));
    }
    if (endTime != null) {
      entity.set(END_TIME_PROPERTY, Timestamp.of(endTime));
    }
    entity.set(CHILD_KEYS_PROPERTY, KeyHelper.convertKeyList(childKeys));
    entity.set(ATTEMPT_NUM_PROPERTY, attemptNumber);
    entity.set(MAX_ATTEMPTS_PROPERTY, maxAttempts);
    entity.set(BACKOFF_SECONDS_PROPERTY, backoffSeconds);
    entity.set(BACKOFF_FACTOR_PROPERTY, backoffFactor);
    if (queueSettings.getOnBackend() != null) {
      entity.set(ON_BACKEND_PROPERTY, queueSettings.getOnBackend());
    }
    if (queueSettings.getOnService() != null) {
      entity.set(ON_SERVICE_PROPERTY, queueSettings.getOnService());
    }
    if (queueSettings.getOnVersion() != null) {
      entity.set(ON_VERSION_PROPERTY, queueSettings.getOnVersion());
    }
    if (queueSettings.getOnQueue() != null) {
      entity.set(ON_QUEUE_PROPERTY, queueSettings.getOnQueue());
    }
    if (queueSettings.getQueueRetryTaskRetryLimit() != null) {
      entity.set(QUEUE_RETRY_TASK_RETRY_LIMIT_PROPERTY, queueSettings.getQueueRetryTaskRetryLimit());
    }
    if (queueSettings.getQueueRetryTaskAgeLimitSeconds() != null) {
      entity.set(QUEUE_RETRY_TASK_AGE_LIMIT_SECONDS_PROPERTY, queueSettings.getQueueRetryTaskAgeLimitSeconds());
    }
    if (queueSettings.getQueueRetryMinBackoffSeconds() != null) {
      entity.set(QUEUE_RETRY_TASK_MIN_BACKOFF_SECONDS_PROPERTY, queueSettings.getQueueRetryMinBackoffSeconds());
    }
    if (queueSettings.getQueueRetryMaxBackoffSeconds() != null) {
      entity.set(QUEUE_RETRY_TASK_MAX_BACKOFF_SECONDS_PROPERTY, queueSettings.getQueueRetryMaxBackoffSeconds());
    }
    if (queueSettings.getQueueRetryMaxDoublings() != null) {
      entity.set(QUEUE_RETRY_TASK_MAX_DOUBLINGS_PROPERTY, queueSettings.getQueueRetryMaxDoublings());
    }
    if (statusConsoleUrl != null) {
      entity.set(STATUS_CONSOLE_URL, statusConsoleUrl);
    }
    if (rootJobDisplayName != null) {
      entity.set(ROOT_JOB_DISPLAY_NAME, rootJobDisplayName);
    }
    return entity.build();
  }

  /**
   * Constructs a new JobRecord given the provided data. The constructed
   * instance will be inflated in the sense that
   * {@link #getJobInstanceInflated()}, {@link #getFinalizeBarrierInflated()},
   * {@link #getOutputSlotInflated()} and {@link #getRunBarrierInflated()} will
   * all not return {@code null}. This constructor is used when a new JobRecord
   * is created during the run() method of a parent job. The parent job is also
   * known as the generator job.
   *
   * @param generatorJob The parent generator job of this job.
   * @param graphGUIDParam The GUID of the local graph of this job.
   * @param jobInstance The non-null user-supplied instance of {@code Job} that
   *        implements the Job that the newly created JobRecord represents.
   * @param callExceptionHandler The flag that indicates that this job should call
   *        {@code Job#handleException(Throwable)} instead of {@code run}.
   * @param settings Array of {@code JobSetting} to apply to the newly created
   *        JobRecord.
   */
  public JobRecord(JobRecord generatorJob, String graphGUIDParam, Job<?> jobInstance,
      boolean callExceptionHandler, JobSetting[] settings) {
    this(generatorJob.getRootJobKey(), null, generatorJob.getKey(), graphGUIDParam, jobInstance,
        callExceptionHandler, settings, generatorJob.getQueueSettings());
    // If generator job has exception handler then it should be called in case
    // of this job throwing to create an exception handling child job.
    // If callExceptionHandler is true then this job is an exception handling
    // child and its exceptions should be handled by its parent's
    // exceptionHandlingAncestor to avoid infinite recursion.
    if (generatorJob.isExceptionHandlerSpecified() && !callExceptionHandler) {
      exceptionHandlingAncestorKey = generatorJob.getKey();
    } else {
      exceptionHandlingAncestorKey = generatorJob.getExceptionHandlingAncestorKey();
    }
    // Inherit settings from generator job
    Map<Class<? extends JobSetting>, JobSetting> settingsMap = new HashMap<>();
    for (JobSetting setting : settings) {
      settingsMap.put(setting.getClass(), setting);
    }
    if (!settingsMap.containsKey(StatusConsoleUrl.class)) {
      statusConsoleUrl = generatorJob.statusConsoleUrl;
    }
  }

  private JobRecord(Key rootJobKey, Key thisKey, Key generatorJobKey, String graphGUID,
      Job<?> jobInstance, boolean callExceptionHandler, JobSetting[] settings,
      QueueSettings parentQueueSettings) {
    super(rootJobKey, null, thisKey, generatorJobKey, graphGUID);
    jobInstanceRecordInflated = new JobInstanceRecord(this, jobInstance);
    jobInstanceKey = jobInstanceRecordInflated.getKey();
    exceptionHandlerSpecified = isExceptionHandlerSpecified(jobInstance);
    this.callExceptionHandler = callExceptionHandler;
    runBarrierInflated = new Barrier(Barrier.Type.RUN, this);
    runBarrierKey = runBarrierInflated.getKey();
    finalizeBarrierInflated = new Barrier(Barrier.Type.FINALIZE, this);
    finalizeBarrierKey = finalizeBarrierInflated.getKey();
    outputSlotInflated = new Slot(getRootJobKey(), getGeneratorJobKey(), getGraphGuid());
    // Initially we set the filler of the output slot to be this Job.
    // During finalize we may reset it to the filler of the finalize slot.
    outputSlotInflated.setSourceJobKey(getKey());
    outputSlotKey = outputSlotInflated.getKey();
    childKeys = new LinkedList<>();
    state = State.WAITING_TO_RUN;
    for (JobSetting setting : settings) {
      applySetting(setting);
    }
    if (parentQueueSettings != null) {
      queueSettings.merge(parentQueueSettings);
    }
    if (queueSettings.getOnBackend() == null) {
      String service = queueSettings.getOnService();
      String version = queueSettings.getOnVersion();
      if (service == null) {
        String currentBackend = getCurrentBackend();
        if (currentBackend != null) {
          queueSettings.setOnBackend(currentBackend);
        } else {
          queueSettings.setOnService(getCurrentService());
          queueSettings.setOnVersion(getCurrentVersion());
        }
      } else if (version == null) {
        if (service.equals(getCurrentService())) {
          queueSettings.setOnVersion(getCurrentVersion());
        } else {
          queueSettings.setOnVersion("default");
        }
      }
    }
  }



  private static String getCurrentBackend() {
    if (Boolean.parseBoolean(System.getenv("GAE_VM"))) {
      // MVM can't be a backend.
      return null;
    }
    BackendService backendService = BackendServiceFactory.getBackendService();
    String currentBackend = backendService.getCurrentBackend();
    // If currentBackend contains ':' it is actually a B type module (see b/12893879)
    if (currentBackend != null && currentBackend.indexOf(':') != -1) {
      currentBackend = null;
    }
    return currentBackend;
  }

  private static String getCurrentService() {
    String service = System.getenv("GAE_SERVICE");
    return service == null ? System.getProperty("GAE_SERVICE") : service;
  }

  private static String getCurrentVersion() {
    String version = System.getenv("GAE_VERSION");
    return version == null ? System.getProperty("GAE_VERSION") : version;
  }

  // Constructor for Root Jobs (called by {@link #createRootJobRecord}).
  private JobRecord(Key key, Job<?> jobInstance, JobSetting[] settings) {
    // Root Jobs have their rootJobKey the same as their keys and provide null for generatorKey
    // and graphGUID. Also, callExceptionHandler is always false.
    this(key, key, null, null, jobInstance, false, settings, null);
    rootJobDisplayName = jobInstance.getJobDisplayName();
  }

  /**
   * A factory method for root jobs.
   *
   * @param jobInstance The non-null user-supplied instance of {@code Job} that
   *        implements the Job that the newly created JobRecord represents.
   * @param settings Array of {@code JobSetting} to apply to the newly created
   *        JobRecord.
   */
  public static JobRecord createRootJobRecord(Job<?> jobInstance, JobSetting[] settings) {
    Key key = generateKey(null, DATA_STORE_KIND);
    return new JobRecord(key, jobInstance, settings);
  }

  public static boolean isExceptionHandlerSpecified(Job<?> jobInstance) {
    boolean result = false;
    Class<?> clazz = jobInstance.getClass();
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(EXCEPTION_HANDLER_METHOD_NAME)) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1 || !Throwable.class.isAssignableFrom(parameterTypes[0])) {
          throw new RuntimeException(method
              + " has invalid signature. It must have exactly one paramter of type "
              + "Throwable or any of its descendants");
        }
        result = true;
        // continue looping to check signature of all handleException methods
      }
    }
    return result;
  }

  private void applySetting(JobSetting setting) {
    if (setting instanceof WaitForSetting) {
      WaitForSetting wf = (WaitForSetting) setting;
      FutureValueImpl<?> fv = (FutureValueImpl<?>) wf.getValue();
      Slot slot = fv.getSlot();
      runBarrierInflated.addPhantomArgumentSlot(slot);
    } else if (setting instanceof IntValuedSetting) {
      int value = ((IntValuedSetting) setting).getValue();
      if (setting instanceof BackoffSeconds) {
        backoffSeconds = value;
      } else if (setting instanceof BackoffFactor) {
        backoffFactor = value;
      } else if (setting instanceof MaxAttempts) {
        maxAttempts = value;
      } else if (setting instanceof QueueRetryTaskRetryLimit) {
        queueSettings.setQueueRetryTaskRetryLimit((long) value);
      } else if (setting instanceof QueueRetryTaskAgeLimitSeconds) {
        queueSettings.setQueueRetryTaskAgeLimitSeconds((long) value);
      } else if (setting instanceof QueueRetryMinBackoffSeconds) {
        queueSettings.setQueueRetryMinBackoffSeconds((long) value);
      } else if (setting instanceof QueueRetryMaxBackoffSeconds) {
        queueSettings.setQueueRetryMaxBackoffSeconds((long) value);
      } else if (setting instanceof QueueRetryMaxDoublings) {
        queueSettings.setQueueRetryMaxDoublings((long) value);
      } else {
        throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
      }
    } else if (setting instanceof OnBackend) {
      queueSettings.setOnBackend(((OnBackend) setting).getValue());
    } else if (setting instanceof OnService) {
      queueSettings.setOnService(((OnService) setting).getValue());
    } else if (setting instanceof OnVersion) {
      queueSettings.setOnVersion(((OnVersion) setting).getValue());
    } else if (setting instanceof OnQueue) {
      queueSettings.setOnQueue(((OnQueue) setting).getValue());
    } else if (setting instanceof StatusConsoleUrl) {
      statusConsoleUrl = ((StatusConsoleUrl) setting).getValue();
    } else {
      throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
    }
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  private static boolean checkForInflate(PipelineModelObject obj, Key expectedGuid, String name) {
    if (null == obj) {
      return false;
    }
    if (!expectedGuid.equals(obj.getKey())) {
      throw new IllegalArgumentException(
          "Wrong guid for " + name + ". Expected " + expectedGuid + " but was " + obj.getKey());
    }
    return true;
  }

  public void inflate(Barrier runBarrier, Barrier finalizeBarrier, Slot outputSlot,
      JobInstanceRecord jobInstanceRecord, ExceptionRecord exceptionRecord) {
    if (checkForInflate(runBarrier, runBarrierKey, "runBarrier")) {
      runBarrierInflated = runBarrier;
    }
    if (checkForInflate(finalizeBarrier, finalizeBarrierKey, "finalizeBarrier")) {
      finalizeBarrierInflated = finalizeBarrier;
    }
    if (checkForInflate(outputSlot, outputSlotKey, "outputSlot")) {
      outputSlotInflated = outputSlot;
    }
    if (checkForInflate(jobInstanceRecord, jobInstanceKey, "jobInstanceRecord")) {
      jobInstanceRecordInflated = jobInstanceRecord;
    }
    if (checkForInflate(exceptionRecord, exceptionKey, "exception")) {
      exceptionInflated = exceptionRecord.getException();
    }
  }

  public Key getRunBarrierKey() {
    return runBarrierKey;
  }

  public Barrier getRunBarrierInflated() {
    return runBarrierInflated;
  }

  public Key getFinalizeBarrierKey() {
    return finalizeBarrierKey;
  }

  public Barrier getFinalizeBarrierInflated() {
    return finalizeBarrierInflated;
  }

  public Key getOutputSlotKey() {
    return outputSlotKey;
  }

  /**
   * Used to set exceptionHandling Job output to the same slot as the protected job.
   */
  public void setOutputSlotInflated(Slot outputSlot) {
    outputSlotInflated = outputSlot;
    outputSlotKey = outputSlot.getKey();
  }

  public Slot getOutputSlotInflated() {
    return outputSlotInflated;
  }

  public Key getJobInstanceKey() {
    return jobInstanceKey;
  }

  public JobInstanceRecord getJobInstanceInflated() {
    return jobInstanceRecordInflated;
  }

  public void setStartTime(Date date) {
    startTime = date;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setEndTime(Date date) {
    endTime = date;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setState(State state) {
    this.state = state;
  }

  public void setChildGraphGuid(String guid) {
    childGraphGuid = guid;
  }

  public State getState() {
    return state;
  }

  public boolean isExceptionHandlerSpecified() {
    // If this job is exception handler itself then it has exceptionHandlerSpecified
    // but it shouldn't delegate to it.
    return exceptionHandlerSpecified && (!isCallExceptionHandler());
  }

  /**
   * Returns key of the nearest ancestor that has exceptionHandler method
   * overridden or <code>null</code> if none of them has it.
   */
  public Key getExceptionHandlingAncestorKey() {
    return exceptionHandlingAncestorKey;
  }

  public Key getExceptionHandlerJobKey() {
    return exceptionHandlerJobKey;
  }

  public String getExceptionHandlerJobGraphGuid() {
    return exceptionHandlerJobGraphGuid;
  }

  public void setExceptionHandlerJobGraphGuid(String exceptionHandlerJobGraphGuid) {
    this.exceptionHandlerJobGraphGuid = exceptionHandlerJobGraphGuid;
  }

  /**
   * If true then this job is exception handler and
   * {@code Job#handleException(Throwable)} should be called instead of <code>run
   * </code>.
   */
  public boolean isCallExceptionHandler() {
    return callExceptionHandler;
  }

  /**
   * If <code>true</code> then an exception during a job execution is ignored. It is
   * expected to be set to <code>true</code> for jobs that execute error handler due
   * to cancellation.
   */
  public boolean isIgnoreException() {
    return ignoreException;
  }

  public void setIgnoreException(boolean ignoreException) {
    this.ignoreException = ignoreException;
  }

  public int getAttemptNumber() {
    return (int) attemptNumber;
  }

  public void incrementAttemptNumber() {
    attemptNumber++;
  }

  public int getBackoffSeconds() {
    return (int) backoffSeconds;
  }

  public int getBackoffFactor() {
    return (int) backoffFactor;
  }

  public int getMaxAttempts() {
    return (int) maxAttempts;
  }

  /**
   * Returns a copy of QueueSettings
   */
  public QueueSettings getQueueSettings() {
    return queueSettings;
  }

  public String getStatusConsoleUrl() {
    return statusConsoleUrl;
  }

  public void setStatusConsoleUrl(String statusConsoleUrl) {
    this.statusConsoleUrl = statusConsoleUrl;
  }

  public void appendChildKey(Key key) {
    childKeys.add(key);
  }

  public List<Key> getChildKeys() {
    return childKeys;
  }

  public String getChildGraphGuid() {
    return childGraphGuid;
  }

  public void setExceptionKey(Key exceptionKey) {
    this.exceptionKey = exceptionKey;
  }

  @Override
  public JobInfo.State getJobState() {
    switch (state) {
      case WAITING_TO_RUN:
      case WAITING_TO_FINALIZE:
        return JobInfo.State.RUNNING;
      case FINALIZED:
        return JobInfo.State.COMPLETED_SUCCESSFULLY;
      case CANCELED:
        return JobInfo.State.CANCELED_BY_REQUEST;
      case STOPPED:
        if (null == exceptionKey) {
          return JobInfo.State.STOPPED_BY_REQUEST;
        } else {
          return JobInfo.State.STOPPED_BY_ERROR;
        }
      case RETRY:
        return JobInfo.State.WAITING_TO_RETRY;
      default:
        throw new RuntimeException("Unrecognized state: " + state);
    }
  }

  @Override
  public Object getOutput() {
    if (null == outputSlotInflated) {
      return null;
    } else {
      return outputSlotInflated.getValue();
    }
  }

  @Override
  public String getError() {
    if (exceptionInflated == null) {
      return null;
    }
    return StringUtils.printStackTraceToString(exceptionInflated);
  }

  @Override
  public Throwable getException() {
    return exceptionInflated;
  }

  public Key getExceptionKey() {
    return exceptionKey;
  }

  public String getRootJobDisplayName() {
    return rootJobDisplayName;
  }

  private String getJobInstanceString() {
    if (null == jobInstanceRecordInflated) {
      return "jobInstanceKey=" + jobInstanceKey;
    }
    String jobClass = jobInstanceRecordInflated.getClassName();
    return jobClass + (callExceptionHandler ? ".handleException" : ".run");
  }

  @Override
  public String toString() {
    return "JobRecord [" + getKeyName(getKey()) + ", " + state + ", " + getJobInstanceString()
        + ", callExceptionJHandler=" + callExceptionHandler + ", runBarrier="
        + runBarrierKey.getName() + ", finalizeBarrier=" + finalizeBarrierKey.getName()
        + ", outputSlot=" + outputSlotKey.getName() + ", rootJobDisplayName="
        + rootJobDisplayName + ", parent=" + getKeyName(getGeneratorJobKey()) + ", guid="
        + getGraphGuid() + ", childGuid=" + childGraphGuid + "]";
  }
}
