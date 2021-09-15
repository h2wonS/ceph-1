#!/usr/bin/python3

import argparse
import subprocess
import os
import shutil
import random
import sys
import time

num_files = 1000
skew_ratio = 20
dedup_ratio = 100
chunk_size = 16384
filepath = os.path.dirname(os.path.abspath(__file__))

def execute_ceph():
  os.chdir(filepath)
  subprocess.call("sudo ./bringup_mon_osd.sh", shell=True)
  
def configure_ceph():
  os.chdir(ceph_bin_abs_path + '/../')
  subprocess.call("sudo bin/ceph osd pool create base_pool 128", shell=True)
  subprocess.call("sudo bin/ceph osd pool create chunk_pool", shell=True)
  subprocess.call("sudo bin/ceph osd set noscrub", shell=True)
  subprocess.call("sudo bin/ceph osd set nodeep-scrub", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool dedup_tier chunk_pool", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool dedup_chunk_algorithm fastcdc", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool dedup_cdc_chunk_size " + str(chunk_size), shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool fingerprint_algorithm sha1", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool target_max_objects 10000", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool target_max_bytes 10485760000", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool pg_autoscale_mode off", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool cache_target_full_ratio .9", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool cache_min_flush_age 40", shell=True)
  subprocess.call("sudo bin/ceph osd pool set base_pool cache_min_evict_age 40", shell=True)
  subprocess.call("sudo bin/rbd create test_rbd --size 100G --pool base_pool", shell=True)
#  subprocess.call("sudo bin/rbd map --pool base_pool test_rbd", shell=True)

def process():
  global ceph_bin_abs_path
  ceph_bin_abs_path = os.path.abspath(args.ceph)
  print ("3. Dedup ratio and metadata according to skew\n")

  for mode in [0, 1]:
    for skew in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
      print("execute ceph\n")
      execute_ceph()
      print("configure ceph\n")
      configure_ceph()

# background statistics profiling
      print("execute profiler\n")
      os.chdir(filepath)
      profiler_process = subprocess.Popen(\
          ["./accumulate_statistics.py",\
          "--ceph", ceph_bin_abs_path,\
          "--pool", "chunk_pool",\
          "--log", "test_03_skew_" + str(skew) +"_mode_" + str(mode) + ".log"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

# execute crawler
      print("execute crawler\n")
      if mode == 0:
          command = "sudo " + ceph_bin_abs_path + "/ceph-dedup-tool --iterative --op sample-dedup --base-pool base_pool --chunk-pool chunk_pool --max-thread 12 --shallow-crawling --sampling-ratio 10 --osd-count 3 --wakeup-period 10 --object-dedup-threshold 40 --chunk-size " + str(chunk_size)
      else:
          command = "sudo " + ceph_bin_abs_path + "/ceph-dedup-tool --iterative --op sample-dedup --base-pool base_pool --chunk-pool chunk_pool --max-thread 12 --sampling-ratio 10 --osd-count 3 --wakeup-period 10 --object-dedup-threshold 40 --chunk-size " + str(chunk_size)
      crawler = subprocess.Popen(command, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

# put objects
      num_unique = 10 - skew
      print("backgroud fio\n")
      fio_log = open("test_03_fio_skew_" + str(skew)+"_mode_"+str(mode)+".log", "w")
      command = "sudo fio --ioengine rbd --clientname admin --pool base_pool --rbdname test_rbd --invalidate 0 --direct 1 --bsrange 4m-4m --time_based --runtime 30 --do_verify 0 --readwrite randwrite --verify_state_save 0 --group_reporting --name unique --verify md5 --numjobs " + str(num_unique)
      for skew_idx in range(skew):
        command += " --name skew_" + str(skew_idx) + " --verify pattern --verify_pattern " + str(skew_idx)
      print(command)
      fio_process = subprocess.Popen(command, shell=True, stdout=fio_log)
   
      print ("wait 600") 
      time.sleep(600)

      profiler_process.terminate()
      profiler_process.wait()
      fio_process.terminate()
      fio_process.wait()
      crawler.kill()
      crawler.wait()
      subprocess.call("sudo pkill -9 dedup-tool", shell=True)
      subprocess.call("sudo pkill -9 fio", shell=True)
      subprocess.call("sudo pkill -9 ceph", shell=True)

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('--ceph', type=str, default='../build/bin/', help='ceph bin path')
  global args
  args = parser.parse_args()

if __name__ == "__main__":
  parse_arguments()
  process()

