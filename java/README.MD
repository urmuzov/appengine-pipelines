To use `CloudTaskQueue` properties `cloudtasks.api.root.url` and `cloudtasks.api.default.parent` should be setted.

If there isn't `cloudtasks.api.root.url` property, will be used `AppEngineTaskQueue`, and queue fails in **standard** environment   
 
Example for **flexible** environment:
```env_variables:
      JAVA_OPTS: >-
        -Dcloudtasks.api.root.url="https://cloudtasks.googleapis.com/"
        -Dcloudtasks.api.default.parent="projects/{applicationId}/locations/{locationId}"
```