package com.google.appengine.tools.development.testing;

import com.gigware.deferred.DeferredTask;
import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.taskqueue.dev.LocalTaskQueueCallback;
import com.google.appengine.api.urlfetch.URLFetchServicePb;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.Map;

public class DeferredTaskCallback implements LocalTaskQueueCallback {
  private static final String CURRENT_NAMESPACE_HEADER = "X-AppEngine-Current-Namespace";

  public DeferredTaskCallback() {
  }

  public void initialize(Map<String, String> properties) {
  }

  public int execute(URLFetchServicePb.URLFetchRequest req) {
    String currentNamespace = NamespaceManager.get();
    String requestNamespace = null;
    ByteString payload = null;
    Iterator var5 = req.getHeaderList().iterator();

    while (true) {
      while (var5.hasNext()) {
        URLFetchServicePb.URLFetchRequest.Header header = (URLFetchServicePb.URLFetchRequest.Header) var5.next();
        if (header.getKey().equals("content-type") && "application/x-binary-app-engine-java-runnable-task".equals(header.getValue())) {
          payload = req.getPayload();
        } else if ("X-AppEngine-Current-Namespace".equals(header.getKey())) {
          requestNamespace = header.getValue();
        }
      }

      boolean namespacesDiffer = requestNamespace != null && !requestNamespace.equals(currentNamespace);
      if (namespacesDiffer) {
        NamespaceManager.set(requestNamespace);
      }

      int var16;
      try {
        if (payload != null) {
          ByteArrayInputStream bais = new ByteArrayInputStream(payload.toByteArray());

          short var9;
          try {
            ObjectInputStream ois = new ObjectInputStream(bais);
            DeferredTask deferredTask = (DeferredTask) ois.readObject();
            HttpServletRequest fakeHttpServletRequest = new FakeHttpServletRequest();
            deferredTask.run(fakeHttpServletRequest);
            var9 = 200;
            return var9;
          } catch (Exception var13) {
            var9 = 500;
            return var9;
          }
        }

        var16 = this.executeNonDeferredRequest(req);
      } finally {
        if (namespacesDiffer) {
          NamespaceManager.set(currentNamespace);
        }

      }

      return var16;
    }
  }

  protected int executeNonDeferredRequest(URLFetchServicePb.URLFetchRequest req) {
    return 200;
  }
}