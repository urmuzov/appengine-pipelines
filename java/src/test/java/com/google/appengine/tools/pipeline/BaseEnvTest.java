package com.google.appengine.tools.pipeline;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.google.appengine.tools.pipeline.impl.util.GUIDGenerator.USE_SIMPLE_GUIDS_FOR_DEBUGGING;

public class BaseEnvTest {
  private static LocalDatastoreHelper localDatastoreHelper = LocalDatastoreHelper.create();
  protected LocalServiceTestHelper helper;
  protected LocalTaskQueue taskQueue;

  public BaseEnvTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);

    helper = new LocalServiceTestHelper(taskQueueConfig, new LocalModulesServiceTestConfig());
  }

  @BeforeClass
  public static void setUpClass() {
    try {
      localDatastoreHelper.start();
      System.setProperty(com.google.datastore.v1.client.DatastoreHelper.LOCAL_HOST_ENV_VAR, localDatastoreHelper.getOptions().getHost());
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  @AfterClass
  public static void tearDownClass() {
    try {
      localDatastoreHelper.stop(Duration.standardSeconds(2));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setUp() throws Exception {
    helper.setUp();
    localDatastoreHelper.reset();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();
  }

  @After
  public void tearDown() throws Exception {
    helper.tearDown();
  }
}
