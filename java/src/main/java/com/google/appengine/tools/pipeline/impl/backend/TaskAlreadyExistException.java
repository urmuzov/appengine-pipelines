package com.google.appengine.tools.pipeline.impl.backend;

public class TaskAlreadyExistException extends RuntimeException {
  public TaskAlreadyExistException() {
  }

  public TaskAlreadyExistException(final String message) {
    super(message);
  }

  public TaskAlreadyExistException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public TaskAlreadyExistException(final Throwable cause) {
    super(cause);
  }

  public TaskAlreadyExistException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
