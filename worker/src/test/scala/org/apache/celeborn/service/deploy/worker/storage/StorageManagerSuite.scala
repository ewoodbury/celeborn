/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.storage

import java.{lang, util}
import java.io.IOException
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.io.FileUtils
import org.mockito.{Mockito, MockitoSugar}
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.stubbing.Stubber

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_DISK_RESERVE_SIZE, WORKER_GRACEFUL_SHUTDOWN_ENABLED, WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, WORKER_STORAGE_DIRS}
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskInfo, DiskStatus, MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType, StorageInfo}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.worker.WorkerSource

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class StorageManagerSuite extends CelebornFunSuite with MockitoHelper {

  test("[CELEBORN-926] saveAllCommittedFileInfosToDB cause IllegalMonitorStateException") {
    val conf = new CelebornConf().set(WORKER_GRACEFUL_SHUTDOWN_ENABLED, true)
      .set(WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, "/tmp/recover")
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    // should not throw IllegalMonitorStateException exception
    storageManager.saveAllCommittedFileInfosToDB()
  }

  test("updateDiskInfosWithDiskReserveSize") {
    // reserve size set to 5g
    val conf = new CelebornConf().set(WORKER_DISK_RESERVE_SIZE, Utils.byteStringAsBytes("5g"))
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    val spyStorageManager = spy(storageManager)

    val diskInfo = new DiskInfo("/mnt/disk1", List.empty, null, conf)
    diskInfo.setUsableSpace(-1L)

    var diskSetSpace = (0L, 0L)
    doReturn(List(diskInfo)).when(spyStorageManager).localDisksSnapshot()
    doAnswer(diskSetSpace).when(spyStorageManager).getFileSystemReportedSpace(any)

    // disk usable 80g, total 80g, worker config 8EB
    diskSetSpace = (80 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = Long.MaxValue
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 75 * 1024 * 1024 * 1024L)

    // disk usable 80g, total 80g, worker config 50g
    diskInfo.configuredUsableSpace = 50 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 50 * 1024 * 1024 * 1024L)

    // disk usable 10g, total 80g, worker config 20g
    diskSetSpace = (10 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = 20 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 5 * 1024 * 1024 * 1024L)

    // disk usable 10g, total 80g, worker config 5g
    diskInfo.configuredUsableSpace = 5 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 5 * 1024 * 1024 * 1024L)

    // disk usable 5g, total 80g, worker config 20g
    diskSetSpace = (5 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = 20 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)

    // disk usable 5g, total 80g, worker config 5g
    diskInfo.configuredUsableSpace = 5 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)

    // disk usable 1g, total 80g, worker config 20g
    diskSetSpace = (1 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = 20 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)

    // disk usable 1g, total 80g, worker config 5g
    diskInfo.configuredUsableSpace = 5 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)
  }

  test("[CELEBORN-2310] Ensure createFile rejected with disks are full, but status is HEALTHY") {
    val conf = new CelebornConf().set(WORKER_DISK_RESERVE_SIZE, Utils.byteStringAsBytes("5g")).set(
      WORKER_STORAGE_DIRS,
      Seq("/"))
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    val spyStorageManager = spy(storageManager)
    val diskInfo = new DiskInfo("/", List.empty, null, conf)
    diskInfo.setUsableSpace(-1L)
    // Should fail even if the status is HEALTHY
    diskInfo.setStatus(DiskStatus.HEALTHY)
    doReturn(List(diskInfo)).when(spyStorageManager).localDisksSnapshot()

    val partitionLocation = genPartitionLocation(0, Array(0L))

    try {
      val file = storageManager.createDiskFile(
        partitionLocation,
        "myAppId",
        0,
        "myFile",
        new UserIdentifier("t1", "u1"),
        PartitionType.REDUCE,
        partitionSplitEnabled = false)
      fail("Should throw IOException when disks are full")
    } catch {
      case e: IOException =>
        assert(e.getMessage.equals(
          s"No available disks! suggested mountPoint ${partitionLocation.getStorageInfo.getMountPoint}"))
      case e: Throwable =>
        fail(s"Should throw IOException, but got ${e.getClass.getSimpleName}", e)
    }
  }

  test("cleanFile destroys an HDFS writer whether or not the shuffle has memory files") {
    val hdfsDir = Files.createTempDirectory("celeborn-hdfs")
    val conf = new CelebornConf()
      .set("celeborn.storage.availableTypes", "MEMORY,HDFS")
      .set("celeborn.storage.hdfs.dir", hdfsDir.toUri.toString)
      .set("celeborn.worker.flusher.hdfs.threads", "1")
      .set("celeborn.hadoop.fs.file.impl.disable.cache", "true")
    val hadoopFsBefore = StorageManager.hadoopFs
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    val userIdentifier = new UserIdentifier("t1", "u1")
    try {
      // shuffle id -> memory files already registered for that shuffle (None: no memory map)
      Seq(1 -> None, 2 -> Some(Seq.empty), 3 -> Some(Seq("1-0-1"))).foreach {
        case (shuffleId, memoryFiles) =>
          val shuffleKey = s"app-$shuffleId"
          val fileName = "0-0-1"
          memoryFiles.foreach { names =>
            val files = new ConcurrentHashMap[String, MemoryFileInfo]()
            names.foreach(files.put(
              _,
              new MemoryFileInfo(userIdentifier, false, new ReduceFileMeta(1024))))
            storageManager.memoryFileInfos.put(shuffleKey, files)
          }
          val location =
            new PartitionLocation(0, 0, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.REPLICA)
          location.setStorageInfo(
            new StorageInfo("", StorageInfo.Type.HDFS, StorageInfo.ALL_TYPES_AVAILABLE_MASK))
          val (_, fileInfo, _) = storageManager.createDiskFile(
            location,
            "app",
            shuffleId,
            fileName,
            userIdentifier,
            PartitionType.REDUCE,
            partitionSplitEnabled = false,
            StorageInfo.Type.HDFS)
          val writer = mock[PartitionDataWriter]
          storageManager.hdfsWriters.put(fileInfo.getFilePath, writer)

          storageManager.cleanFile(shuffleKey, fileName)

          withClue(s"memory files $memoryFiles") {
            verify(writer).destroy(any[IOException])
            assert(storageManager.getFileInfo(shuffleKey, fileName) == null)
          }
      }
    } finally {
      // StorageManager.close() would also shut down the static device check pool other suites use.
      StorageManager.hadoopFs.get(StorageInfo.Type.HDFS).close()
      StorageManager.hadoopFs = hadoopFsBefore
      FileUtils.deleteDirectory(hdfsDir.toFile)
    }
  }

  private def genPartitionLocation(epoch: Int, offsets: Array[Long]): PartitionLocation = {
    val location: PartitionLocation =
      new PartitionLocation(0, epoch, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY)
    val storageInfo: StorageInfo = new StorageInfo(
      StorageInfo.Type.HDD,
      "/",
      false,
      "filePath",
      StorageInfo.ALL_TYPES_AVAILABLE_MASK,
      offsets(offsets.length - 1),
      new util.ArrayList[lang.Long]())
    location.setStorageInfo(storageInfo)
    location
  }
}
