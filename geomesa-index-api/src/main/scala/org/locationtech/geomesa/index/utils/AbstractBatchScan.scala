/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent._

import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool

import scala.util.control.NonFatal

/**
  * Provides parallelism for scanning multiple ranges at a given time, for systems that don't
  * natively support that. 【并行的Scan】
  *
  * Subclasses should generally only expose the `CloseableIterator` interface, and make sure to
  * invoke `start()` before returning to the caller.
  *
  * @param ranges ranges to scan
  * @param threads number of client threads to use for scanning
  * @param buffer max size of the buffer for storing results before they are read by the caller
  * @param sentinel singleton sentinel value used to indicate the completion of scanning threads【哨兵对象】
  * @tparam T range type
  * @tparam R scan result type
  */
abstract class AbstractBatchScan[T, R <: AnyRef](ranges: Seq[T], threads: Int, buffer: Int, sentinel: R)
    extends CloseableIterator[R] {

  import scala.collection.JavaConverters._

  require(threads > 0, "Thread count must be greater than 0")

  private val inQueue = new ConcurrentLinkedQueue(ranges.asJava)
  private val outQueue = new LinkedBlockingQueue[R](buffer)

  private val pool = new CachedThreadPool(threads)  //基于JDK的AbstractExecutorService实现的线程池
  private val latch = new CountDownLatch(threads)  //锁存器，低阶别、轻量级的锁
  private val terminator = new Terminator()

  private var retrieved: R = _
  private var error: Throwable = _

  @volatile
  protected var closed: Boolean = false

  /**
    * Scan a single range
    * 继承后自行实现具体的Scan方法
    * @param range range to scan
    */
  protected def scan(range: T): CloseableIterator[R]

  /**
    * Start the threaded scans executing
    */
  protected def start(): CloseableIterator[R] = {
    var i = 0
    while (i < threads) {
      pool.submit(new SingleThreadScan())  //提交一个任务
      i += 1
    }
    pool.submit(terminator)
    pool.shutdown()
    this
  }

  override def hasNext: Boolean = {
    if (retrieved != null) {
      true  //判断任务是否可取
    } else {
      retrieved = outQueue.take
      if (!retrieved.eq(sentinel)) {
        true
      } else {
        outQueue.put(sentinel) // re-queue in case hasNext is called again
        retrieved = null.asInstanceOf[R]
        this.synchronized {
          if (error != null) {
            throw error
          }
        }
        false
      }
    }
  }

  override def next(): R = {
    val n = retrieved
    retrieved = null.asInstanceOf[R]
    n
  }

  override def close(): Unit = {
    closed = true
    inQueue.clear()
    terminator.terminate(true)
  }

  /**
    * Exposed for testing only
    *
    * @param timeout timeout to wait
    * @return true if all threads have terminated, otherwise false
    */
  private [utils] def waitForDone(timeout: Long): Boolean = {
    val start = System.currentTimeMillis()
    while (true) {
      if (pool.isTerminated) {
        return true  //多线程运行的任务是否完成
      } else if (System.currentTimeMillis() - start > timeout) {
        return false
      } else {
        Thread.sleep(10)
      }
    }
    throw new IllegalStateException() // not possible to hit this, but the compiler can't figure that out
  }

  /**
    * Exposed for testing only
    *
    * @param timeout timeout to wait
    * @return true if full, otherwise false
    */
  private [utils] def waitForFull(timeout: Long): Boolean = {
    val start = System.currentTimeMillis()
    while (true) {
      if (outQueue.remainingCapacity() == 0) {
        return true   //结果队列如果剩余容量为空了表示已经Full
      } else if (System.currentTimeMillis() - start > timeout) {
        return false
      } else {
        Thread.sleep(10)
      }
    }
    throw new IllegalStateException() // not possible to hit this, but the compiler can't figure that out
  }

  /**
    * Pulls ranges off the queue and executes them
    */
  private class SingleThreadScan extends Runnable {
    override def run(): Unit = {
      try {
        var range = inQueue.poll()  //出队一个查询
        while (range != null) {
          val result = scan(range)
          try {
            while (result.hasNext) {
              val r = result.next
              while (!outQueue.offer(r, 100, TimeUnit.MILLISECONDS)) {
                if (closed) {
                  return
                }
              }
            }
          } finally {
            result.close()
          }
          range = inQueue.poll()  //不断出队直到结束
        }
      } catch {
        case NonFatal(e) =>
          AbstractBatchScan.this.synchronized {
            if (error == null) { error = e } else { error.addSuppressed(e) }
          }
          close()
      } finally {
        latch.countDown()  //锁释放
      }
    }
  }

  /**
    * Injects the terminal value into the output buffer, once all the scans are complete
    */
  private class Terminator extends Runnable {

    @volatile
    private var done = false

    override def run(): Unit = try { latch.await() } finally { terminate(false) }

    final def terminate(drop: Boolean): Unit = {
      // it's possible that the queue is full, in which case we can't immediately
      // add the sentinel to the queue to indicate to the client that scans are done
      // if the scan has been closed, then the client is done
      // reading and we don't mind dropping some results
      // otherwise we wait and give the client a chance to empty the queue
      if (!done && (drop || closed || !outQueue.offer(sentinel, 1000, TimeUnit.MILLISECONDS))) {
        // terminate with drops
        while (!outQueue.offer(sentinel)) { outQueue.poll() }
      }
      done = true
    }
  }
}
