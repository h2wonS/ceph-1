// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// 
// A simple allocator that just hands out space from the next empty zone.  This
// is temporary, just to get the simplest append-only write workload to work.
//
// Copyright (C) 2020 Abutalib Aghayev
//

#include "ZonedAllocator.h"
#include "bluestore_types.h"
#include "zoned_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "ZonedAllocator(" << this << ") " << __func__ << " "

ZonedAllocator::ZonedAllocator(CephContext* cct,
			       int64_t size,
			       int64_t blk_size,
			       int64_t _zone_size,
			       int64_t _first_sequential_zone,
			       std::string_view name)
    : Allocator(name, size, blk_size),
      cct(cct),
      size(size),
      conventional_size(_first_sequential_zone * _zone_size),
      sequential_size(size - conventional_size),
      num_sequential_free(0),
      block_size(blk_size),
      zone_size(_zone_size),
      first_seq_zone_num(_first_sequential_zone),
      starting_zone_num(first_seq_zone_num),
      num_zones(size / zone_size)
{
  ldout(cct, 10) << " size 0x" << std::hex << size
		 << ", zone size 0x" << zone_size << std::dec
		 << ", number of zones 0x" << num_zones
		 << ", first sequential zone 0x" << starting_zone_num
		 << ", sequential size 0x" << sequential_size
		 << std::dec
		 << dendl;
  ceph_assert(size % zone_size == 0);

 zone_states.resize(num_zones);
  for (uint64_t i = 0 ; i < max_open_zone ; i++) {
    active_zones[i] = i + first_seq_zone_num;
  }
  last_visited_idx = max_open_zone - 1;
}

ZonedAllocator::~ZonedAllocator()
{
}

void ZonedAllocator::select_other_zone(uint64_t index) {
  uint64_t current_zone = active_zones[index];
  uint64_t new_zone = (current_zone + max_open_zone) % num_zones;
  active_zones[index] = new_zone;
}

int64_t ZonedAllocator::allocate(
  uint64_t want_size,
  uint64_t alloc_unit,
  uint64_t max_alloc_size,
  int64_t hint,
  PExtentVector *extents)
{
  std::lock_guard l(lock);

  ceph_assert(want_size % 4096 == 0);

  ldout(cct, 10) << " trying to allocate 0x"
		 << std::hex << want_size << std::dec << dendl;
  uint64_t remaining_size = want_size;
  while(remaining_size > 0) {
    uint64_t target_idx = (last_visited_idx + 1) % max_open_zone;
    uint64_t zone_num = active_zones[target_idx];
    if (zone_num == cleaning_zone) {
      select_other_zone(target_idx);
      last_visited_idx++;
      continue;
    }
    uint64_t target_size = remaining_size > interleaving_unit ? interleaving_unit : remaining_size;
    if (!fits(target_size, zone_num)) {
      target_size = get_remaining_space(zone_num);
    }
    uint64_t offset = get_offset(zone_num);
    increment_write_pointer(zone_num, target_size);
    num_sequential_free -= target_size;
    if (get_remaining_space(zone_num) == 0) {
      select_other_zone(target_idx);
    }

    extents->emplace_back(bluestore_pextent_t(offset, target_size));
    remaining_size -= target_size;
    last_visited_idx++;
  }
  return want_size;
}

void ZonedAllocator::release(const interval_set<uint64_t>& release_set)
{
  std::lock_guard l(lock);
  for (auto p = cbegin(release_set); p != cend(release_set); ++p) {
    auto offset = p.get_start();
    auto length = p.get_len();
    uint64_t zone_num = offset / zone_size;
    ldout(cct, 10) << " 0x" << std::hex << offset << "~" << length
		   << " from zone 0x" << zone_num << std::dec << dendl;
    uint64_t num_dead = std::min(zone_size - offset % zone_size, length);
    for ( ; length; ++zone_num) {
      increment_num_dead_bytes(zone_num, num_dead);
      length -= num_dead;
      num_dead = std::min(zone_size, length);
    }
  }
}

uint64_t ZonedAllocator::get_free()
{
  return num_sequential_free;
}

void ZonedAllocator::dump()
{
  std::lock_guard l(lock);
}

void ZonedAllocator::dump(std::function<void(uint64_t offset,
					     uint64_t length)> notify)
{
  std::lock_guard l(lock);
}

void ZonedAllocator::init_from_zone_pointers(
  std::vector<zone_state_t> &&_zone_states)
{
  // this is called once, based on the device's zone pointers
  std::lock_guard l(lock);
  ldout(cct, 10) << dendl;
  zone_states = std::move(_zone_states);
  num_sequential_free = 0;
  for (size_t i = first_seq_zone_num; i < num_zones; ++i) {
    num_sequential_free += zone_size - (zone_states[i].write_pointer % zone_size);
  }
  ldout(cct, 10) << "free 0x" << std::hex << num_sequential_free
		 << " / 0x" << sequential_size << std::dec
		 << dendl;
}

int64_t ZonedAllocator::pick_zone_to_clean(float min_score, uint64_t min_saved)
{
  std::lock_guard l(lock);
  int32_t best = -1;
  float best_score = 0.0;
  for (size_t i = first_seq_zone_num; i < num_zones; ++i) {
    if (zone_states[i].write_pointer < zone_size) {
      continue;
    }
    if (zone_states[i].write_pointer == 0) {
      continue;
    }
    // value (score) = benefit / cost
    //    benefit = how much net free space we'll get (dead bytes)
    //    cost = how many bytes we'll have to rewrite (live bytes)
    // avoid divide by zero on a zone with no live bytes
    float score =
      (float)zone_states[i].num_dead_bytes /
      (float)(zone_states[i].get_num_live_bytes() + 1);
    if (score > 0) {
      ldout(cct, 20) << " zone 0x" << std::hex << i
		     << " dead 0x" << zone_states[i].num_dead_bytes
		     << " score " << score
		     << dendl;
    }
    if (zone_states[i].num_dead_bytes < min_saved) {
      continue;
    }
    if (best < 0 || score > best_score) {
      best = i;
      best_score = score;
    }
  }
  if (best_score >= min_score) {
    ldout(cct, 10) << " zone 0x" << std::hex << best << " with score " << best_score
		   << ": 0x" << zone_states[best].num_dead_bytes
		   << " dead and 0x"
		   << zone_states[best].write_pointer - zone_states[best].num_dead_bytes
		   << " live bytes" << std::dec << dendl;
  } else if (best > 0) {
    ldout(cct, 10) << " zone 0x" << std::hex << best << " with score " << best_score
		   << ": 0x" << zone_states[best].num_dead_bytes
		   << " dead and 0x"
		   << zone_states[best].write_pointer - zone_states[best].num_dead_bytes
		   << " live bytes" << std::dec
		   << " but below min_score " << min_score
		   << dendl;
    best = -1;
  } else {
    ldout(cct, 10) << " no zones found that are good cleaning candidates" << dendl;
  }
  return best;
}

void ZonedAllocator::reset_zone(uint32_t zone)
{
  num_sequential_free += zone_states[zone].write_pointer;
  zone_states[zone].reset();
}

bool ZonedAllocator::low_on_space(void)
{
  std::lock_guard l(lock);
  double free_ratio = static_cast<double>(num_sequential_free) / sequential_size;

  ldout(cct, 10) << " free 0x" << std::hex << num_sequential_free
		 << "/ 0x" << sequential_size << std::dec
		 << ", free ratio is " << free_ratio << dendl;
  ceph_assert(num_sequential_free <= (int64_t)sequential_size);

  // TODO: make 0.25 tunable
  return free_ratio <= 0.25;
}

void ZonedAllocator::shutdown()
{
  ldout(cct, 1) << dendl;
}
