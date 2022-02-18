// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 * Copyright (C) 2020 Abutalib Aghayev
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "HMSMRDevice.h"
extern "C" {
#include <libzbd/zbd.h>
}
#include "common/debug.h"
#include "common/errno.h"
#include "common/blkdev.h"
#include "os/bluestore/BlueStore.h"
#include "blk/kernel/io_uring.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "smrbdev(" << this << " " << path << ") "

using namespace std;

struct hmsmr_cbpriv {
  BlockDevice::aio_callback_t cb;
  void* cbpriv;
  HMSMRDevice* bdev;
  hmsmr_cbpriv(BlockDevice::aio_callback_t cb, void* cbpriv, HMSMRDevice* bdev)
  : cb(cb),
    cbpriv(cbpriv),
    bdev(bdev) { }
};

static uint64_t get_zone(uint64_t offset, uint64_t zone_size) {
  return offset / zone_size;
}

static void hmsmr_cb(void* priv, void* priv2)
{
  hmsmr_cbpriv* casted_priv = static_cast<hmsmr_cbpriv*>(priv);
  aio_t* aio = static_cast<aio_t*>(priv2);
  IOContext* ioc = static_cast<IOContext*>(aio->priv);
  HMSMRDevice* bdev = casted_priv->bdev;

  if (aio->iocb.aio_lio_opcode == IO_CMD_PWRITEV) {
    uint64_t zone = get_zone(aio->offset, bdev->get_zone_size());
    if (bdev->need_alloc_submit_sync()) {
      bdev->do_aio_submit(zone, true);
    } else {
      uint64_t post_zone = get_zone(*aio->post_offset_ptr, bdev->get_zone_size());
      ceph_assert(zone == post_zone);
    }
  }

  if (ioc->num_pending == 0 && ioc->num_running == 0) {
    casted_priv->cb(casted_priv->cbpriv, ioc->priv);
    ioc->pending_aios.clear();
    ioc->post_addrs.clear();
  }
}

HMSMRDevice::HMSMRDevice(CephContext* cct,
    aio_callback_t cb,
    void *cbpriv,
    aio_callback_t d_cb,
    void *d_cbpriv)
  : KernelDevice(cct, hmsmr_cb, new hmsmr_cbpriv(cb, cbpriv, this), d_cb, d_cbpriv)
{
}

bool HMSMRDevice::support(const std::string& path)
{
  return zbd_device_is_zoned(path.c_str()) == 1;
}

int HMSMRDevice::_post_open()
{
  dout(10) << __func__ << dendl;
  zbd_info info;

  zbd_fd = zbd_open(path.c_str(), O_RDWR | O_DIRECT | O_LARGEFILE, &info);
  legacy_fd = ::open(path.c_str(), O_RDWR | O_DIRECT | O_LARGEFILE);

  int r;
  if (zbd_fd < 0) {
    r = errno;
    derr << __func__ << " zbd_open failed on " << path << ": "
      << cpp_strerror(r) << dendl;
    return -r;
  }

  nr_zones = info.nr_zones;
  zone_size = info.zone_size;

  if (cct->_conf->contains("bluestore_cns_path")) {
    string cns_path = cct->_conf->bluestore_cns_path;
    cns_fd = ::open(cns_path.c_str(), O_RDWR | O_DIRECT | O_LARGEFILE);
    if (cns_fd < 0) {
      r = errno;
      derr << __func__ << " cns open failed on " << path << ": "
        << cpp_strerror(r) << dendl;
      goto fail;
    }
    BlkDev bdev(cns_fd);
    int64_t conv_size; 
    r = bdev.get_size(&conv_size);
    conventional_region_size = static_cast<uint64_t>(conv_size);
    if (r < 0) {
      goto fail;
    }
  }
  else {
    conventional_region_size = nr_zones * zone_size;
  }

  pending_ios = new zone_pending_io_t[nr_zones];

  dout(10) << __func__ << " setting zone size to " << zone_size
    << " and conventional region size to " << conventional_region_size
    << " size: " << get_size() << dendl;

  return 0;

fail:
  zbd_close(zbd_fd);
  zbd_fd = -1;
  if (cns_fd >= 0) {
    ::close(cns_fd);
    cns_fd = -1;
  }
  return r;
}

void HMSMRDevice::_pre_close()
{
  if (pending_ios) {
    delete [] pending_ios;
  }
  if (zbd_fd >= 0) {
    zbd_close(zbd_fd);
    zbd_fd = -1;
  }
  if (cns_fd >= 0) {
    ::close(cns_fd);
    cns_fd = -1;
  }
}

void HMSMRDevice::reset_all_zones()
{
  dout(10) << __func__ << dendl;
  zbd_reset_zones(zbd_fd, conventional_region_size, 0);
}

void HMSMRDevice::reset_zone(uint64_t zone)
{
  dout(10) << __func__ << " zone 0x" << std::hex << zone << std::dec << dendl;
  blk_zone_range range = {.sector = zone * zone_size / 512, .nr_sectors = zone_size / 512};
  if (ioctl(legacy_fd, BLKRESETZONE, &range) < 0) {
    derr << __func__ << " resetting zone failed for zone 0x" << std::hex
      << zone << std::dec << dendl;
    ceph_abort("zbd_reset_zones failed");

  }
}

std::vector<uint64_t> HMSMRDevice::get_zones()
{
  std::vector<zbd_zone> zones;
  unsigned int num_zones = size / zone_size;
  zones.resize(num_zones);

  int r = zbd_report_zones(zbd_fd, 0, 0, ZBD_RO_ALL, zones.data(), &num_zones);
  if (r != 0) {
    derr << __func__ << " zbd_report_zones failed on " << path << ": "
      << cpp_strerror(errno) << dendl;
    ceph_abort("zbd_report_zones failed");
  }

  std::vector<uint64_t> wp(num_zones);
  for (unsigned i = 0; i < num_zones; ++i) {
    wp[i] = zones[i].wp;
  }
  return wp;
}

void HMSMRDevice::aio_submit(IOContext* ioc) {
  if (ioc->pending_aios.front().iocb.aio_lio_opcode == IO_CMD_PWRITEV) {
    std::list<uint64_t> requested_zones;
    for (auto pending_aio = ioc->pending_aios.begin();
        pending_aio != ioc->pending_aios.end();
        ++pending_aio) {
      uint64_t zone = get_zone(pending_aio->offset, zone_size);
      pending_aio->priv = static_cast<IOContext*>(ioc);
      if (need_alloc_submit_sync()) {
        std::lock_guard lock(pending_ios[zone].lock);
        aio_t aio_to_push = *pending_aio;
        pending_ios[zone].aios.push_back(aio_to_push);
        requested_zones.push_back(zone);
      } else {
        IOContext::addr_t addr_to_push = {.length = pending_aio->length};
        ioc->post_addrs.push_back(addr_to_push);
        pending_aio->post_offset_ptr = &(ioc->post_addrs.back().offset);
        pending_aio->offset = zone * zone_size;
      }
    }
    if (need_alloc_submit_sync()) {
      for (auto zone = requested_zones.begin();
          zone != requested_zones.end();
          ++zone) {
        do_aio_submit(*zone, false);
      }
    } else {
      KernelDevice::aio_submit(ioc);
    }
  }
  else {
    KernelDevice::aio_submit(ioc);
  }
  return;
}

void HMSMRDevice::do_aio_submit(uint64_t zone, bool completed) {
  std::lock_guard lock(pending_ios[zone].lock);
  if (completed) {
    auto aio = pending_ios[zone].aios.front();
    dout(10) << __func__ << " completed offset " <<  aio.offset << " length " << aio.length <<  dendl;
    pending_ios[zone].aios.pop_front();
    pending_ios[zone].running = false;
  } else {
    if (pending_ios[zone].running) {
      dout(10) << __func__ << " still running zone " <<  zone <<  dendl;
      return;
    }
  }
  if (pending_ios[zone].aios.size() == 0) {
    dout(10) << __func__ << " no pending aio zone " <<  zone <<  dendl;
    return;
  }

  auto pending_aio = pending_ios[zone].aios.begin();
  if (need_alloc_submit_sync() == false) {
    dout(10) << __func__ << " offset " << pending_aio->offset << " to zone aligned " << zone * zone_size << dendl;
    pending_aio->offset = zone * zone_size;
  }
  auto ioc = static_cast<IOContext*>(pending_aio->priv);
  dout(10) << __func__ << " target pending ioc" <<  ioc <<  dendl;

  dout(10) << __func__ << " off " << std::hex << pending_aio->offset << " len " << std::hex << pending_aio->length << dendl;
  dout(10) << __func__ << " before submission ioc " << ioc
    << " pending " << ioc->num_pending.load()
    << " running " << ioc->num_running.load() <<dendl;

  ioc->num_running++;
  ioc->num_pending--;

  dout(10) << __func__ << " after submission ioc " << ioc
    << " pending " << ioc->num_pending.load()
    << " running " << ioc->num_running.load() <<dendl;

  void* priv = static_cast<void*>(ioc);
  int r, retries = 0;
  auto end = std::next(pending_aio, 1);
  int pending = 1;
  assert(pending <= std::numeric_limits<uint16_t>::max());
  r = io_queue->submit_batch(pending_aio, end,
      pending, priv, &retries);

  if (retries)
    derr << __func__ << " retries " << retries << dendl;
  if (r < 0) {
    derr << " aio submit got " << cpp_strerror(r) << dendl;
    ceph_assert(r == 0);
  }

  pending_ios[zone].running = true;

  return;
}
