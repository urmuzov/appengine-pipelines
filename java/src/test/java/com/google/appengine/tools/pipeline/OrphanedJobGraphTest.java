// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.KeyHelper;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.apphosting.api.ApiProxy;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.appengine.tools.pipeline.impl.util.TestUtils.getFailureProperty;

/**
 * A test of the ability of the Pipeline framework to handle orphaned jobs.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class OrphanedJobGraphTest extends PipelineTest {

  private AppEngineBackEnd backend;

  @Override
  protected boolean isHrdSafe() {
    return false;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    backend = new AppEngineBackEnd(new AppEngineTaskQueue(), DatastoreOptions.getDefaultInstance().getService());
    GeneratorJob.runCount.set(0);
    ChildJob.runCount.set(0);
    SupplyPromisedValueRunnable.orphanedObjectExcetionCount.set(0);
  }

  /**
   * Tests the Pipeline frameworks ability to handle a failure during the
   * handling of a RunJob task just before the final transaction.
   * <p>
   * A generator job will run, a child job graph will be persisted, but an
   * exception will be thrown before the generator job can be transactionally
   * saved. The child job graph will thus be orphaned. We test that the tasks
   * that would activate the child job graph will never get enqueued and so the
   * orphaned child job will never run. The generator job will be retried a
   * second time with the same result. The generator job will be retried a third
   * time and it will succeed. Only the third child job graph is non-orphaned
   * and should be activated by tasks and run. The orphaned jobs will be cleaned
   * up when the Pipeline is deleted.
   */
  @Test
  public void testOrphanedJobGraph() throws Exception {
    doOrphanedJobGraphTest(false);
  }

  /**
   * Tests that the method
   * {@link PipelineService#submitPromisedValue(String, Object)} behaves
   * properly if the promise handle has been orphaned.
   * <p>
   * This test is similar to {@link #testOrphanedJobGraph()} except that this
   * time the child job graph is supposed to be activated asynchronously vai a
   * promised value. We test that
   * {@link PipelineService#submitPromisedValue(String, Object)} will throw a
   * {@link OrphanedObjectException} when {@code submitPromisedValue()} is
   * invoked on an orphaned promise handle.
   */
  @Test
  public void testOrphanedJobGraphWithPromisedValue() throws Exception {
    doOrphanedJobGraphTest(true);
  }

  /**
   * Performs the common testing logic for {@link #testOrphanedJobGraph()} and
   * {@link #testOrphanedJobGraphWithPromisedValue()}.
   *
   * @param usePromisedValue Should the child job be activated via a promised
   *                         value. If this is {@code false} then the child job is activated via
   *                         an immediate value.
   */
  private void doOrphanedJobGraphTest(boolean usePromisedValue) throws Exception {

    // Run GeneratorJob
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineHandle = service.startNewPipeline(new GeneratorJob(usePromisedValue));
    waitForJobToComplete(pipelineHandle);

    // The GeneratorJob run() should have failed twice just before the final
    // transaction and succeeded a third time
    Assert.assertTrue(GeneratorJob.runCount.get() >= 2);

    // The ChildJob should only have been run once. The two orphaned jobs were
    // never run
    Assert.assertEquals(1, ChildJob.runCount.get());

    // Get all of the Pipeline objects so we can confirm the orphaned jobs are
    // really there
    PipelineObjects allObjects = PipelineManager.queryFullPipeline(pipelineHandle);
    Key rootJobKey = KeyHelper.createKey(JobRecord.DATA_STORE_KIND, pipelineHandle);
    JobRecord rootJob = allObjects.jobs.get(rootJobKey);
    Assert.assertNotNull(rootJob);
    String graphGuid = rootJob.getChildGraphGuid();
    Assert.assertNotNull(graphGuid);
    int numJobs = allObjects.jobs.size();
    Assert.assertEquals(2, numJobs);
    int numOrphanedJobs = 0;
    int numNonOrphanedJobs = 0;

    // Look through all of the JobRecords in the data store
    for (JobRecord record : allObjects.jobs.values()) {
      // They all have the right rooJobKey
      Assert.assertEquals(rootJobKey, record.getRootJobKey());
      if (record.getKey().equals(rootJobKey)) {
        // This one is the root job
        Assert.assertNull(record.getGraphGuid());
        Assert.assertNull(record.getGeneratorJobKey());
        continue;
      }
      // If its not the root job then it was generated by the root job
      Assert.assertEquals(rootJobKey, record.getGeneratorJobKey());

      // Count the generated jobs that are orphaned and not orphaned
      if (graphGuid.equals(record.getGraphGuid())) {
        numNonOrphanedJobs++;
      } else {
        numOrphanedJobs++;
      }
    }

    // There should be one non-orphaned and at least two orphaned
    Assert.assertEquals(1, numNonOrphanedJobs);
    Assert.assertEquals(0, numOrphanedJobs);

    if (usePromisedValue) {
      // If we are using promised-value activation then an
      // OrphanedObjectException should have been caught at least twice.
      int orphanedObjectExcetionCount =
              SupplyPromisedValueRunnable.orphanedObjectExcetionCount.get();
      Assert.assertTrue("Was expecting orphanedObjectExcetionCount to be more than one, but it was "
              + orphanedObjectExcetionCount, orphanedObjectExcetionCount >= 2);
    }

    // Now delete the whole Pipeline
    service.deletePipelineRecords(pipelineHandle);

    // Check that all jobs have been deleted
    Iterable<Entity> jobs = backend.queryAll(JobRecord.DATA_STORE_KIND, rootJobKey);
    numJobs = 0;
    // TODO(user): replace with Iterables.size once b/11899553 is fixed
    for (@SuppressWarnings("unused") Entity entity : jobs) {
      numJobs++;
    }
    Assert.assertEquals(0, numJobs);
  }

  /**
   * This job will fail just before the final transaction the first two times it
   * is attempted, leaving an orphaned child job graph each time. It will
   * succeed the third time. The job also counts the number of times it was run.
   */
  @SuppressWarnings("serial")
  private static class GeneratorJob extends Job0<Void> {

    private static final String SHOULD_FAIL_PROPERTY =
            getFailureProperty("AppEngineBackeEnd.saveWithJobStateCheck.beforeFinalTransaction");
    public static AtomicInteger runCount = new AtomicInteger(0);
    boolean usePromise;

    public GeneratorJob(boolean usePromise) {
      this.usePromise = usePromise;
    }

    @Override
    public Value<Void> run() {
      int count = runCount.getAndIncrement();
      if (count < 2) {
        System.setProperty(SHOULD_FAIL_PROPERTY, "true");
      }
      Value<Integer> dummyValue;
      if (usePromise) {
        PromisedValue<Integer> promisedValue = newPromise();
        (new Thread(new SupplyPromisedValueRunnable(ApiProxy.getCurrentEnvironment(),
                promisedValue.getHandle()))).start();
        dummyValue = promisedValue;
      } else {
        dummyValue = immediate(0);
      }
      futureCall(new ChildJob(), dummyValue);
      return null;
    }
  }

  /**
   * This job simply counts the number of times it was run.
   */
  @SuppressWarnings("serial")
  private static class ChildJob extends Job1<Void, Integer> {
    public static AtomicInteger runCount = new AtomicInteger(0);

    @Override
    public Value<Void> run(Integer ignored) {
      runCount.incrementAndGet();
      return null;
    }
  }

  /**
   * A {@code Runnable} for invoking the method
   * {@link PipelineService#submitPromisedValue(String, Object)}.
   */
  private static class SupplyPromisedValueRunnable implements Runnable {

    public static AtomicInteger orphanedObjectExcetionCount = new AtomicInteger(0);
    private String promiseHandle;
    private ApiProxy.Environment apiProxyEnvironment;

    public SupplyPromisedValueRunnable(ApiProxy.Environment environment, String promiseHandle) {
      this.promiseHandle = promiseHandle;
      this.apiProxyEnvironment = environment;
    }

    @Override
    public void run() {
      PipelineService service = PipelineServiceFactory.newPipelineService();
      ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
      // TODO(user): Try something better than sleep to make sure
      // this happens after the processing the caller's runTask
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e1) {
        // ignore - use uninterruptables
      }
      try {
        service.submitPromisedValue(promiseHandle, 0);
      } catch (NoSuchObjectException e) {
        throw new RuntimeException(e);
      } catch (OrphanedObjectException f) {
        orphanedObjectExcetionCount.incrementAndGet();
      }
    }
  }
}
