import birl
import birl/duration
import bravo
import bravo/uset.{type USet}
import gleam/bit_array
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order.{Eq, Gt}
import gleam/pair
import types.{type Entry, type RespData, Entry, Null}

const empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

pub fn new() {
  uset.new("kv_store", 1, bravo.Public)
}

pub fn get(set: USet(#(RespData, Entry)), key: RespData) {
  uset.lookup(set, key)
  |> option.map(fn(kv_pair) {
    let Entry(value, expiration) = pair.second(kv_pair)
    let now = birl.utc_now()
    case option.map(expiration, birl.compare(now, _)) {
      Some(Gt) | Some(Eq) -> None
      _ -> Some(value)
    }
  })
  |> option.flatten()
  |> option.unwrap(Null)
}

pub fn set(set: USet(#(RespData, Entry)), key: RespData, entry: Entry) {
  uset.insert(set, [#(key, entry)])
}

pub fn delete(set: USet(#(RespData, Entry)), key: RespData) {
  uset.delete_key(set, key)
}

pub fn dump(_set: USet(#(RespData, Entry))) {
  empty_rdb |> bit_array.base16_decode
}

/// Returns all key-value pairs with TTL in milliseconds
/// Format: List of #(key, value, ttl_ms) where ttl_ms is None for no expiry
pub fn snapshot(
  set: USet(#(RespData, Entry)),
) -> List(#(RespData, RespData, Option(Int))) {
  let now = birl.utc_now()
  uset.tab2list(set)
  |> list.filter_map(fn(kv_pair) {
    let #(key, Entry(value, expiration)) = kv_pair
    // Skip expired keys
    case option.map(expiration, birl.compare(now, _)) {
      Some(Gt) | Some(Eq) -> Error(Nil)
      _ -> {
        // Calculate remaining TTL in milliseconds
        let ttl_ms = case expiration {
          None -> None
          Some(exp_time) -> {
            let diff = birl.difference(exp_time, now)
            Some(duration.blur_to(diff, duration.MilliSecond))
          }
        }
        Ok(#(key, value, ttl_ms))
      }
    }
  })
}

/// Loads snapshot entries into the store
pub fn load_snapshot(
  uset: USet(#(RespData, Entry)),
  entries: List(#(RespData, RespData, Option(Int))),
) {
  let now = birl.utc_now()
  list.each(entries, fn(entry) {
    let #(key, value, ttl_ms) = entry
    let expiration = case ttl_ms {
      None -> None
      Some(ms) -> Some(birl.add(now, duration.milli_seconds(ms)))
    }
    set(uset, key, Entry(value, expiration))
  })
}
