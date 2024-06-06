import birl.{type Time}
import carpenter/table.{type Set}
import gleam/bit_array
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order.{Eq, Gt}
import gleam/pair
import gleam/result
import types.{type Entry, type RespData, BulkString, Entry}

const empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

pub fn new() {
  table.build("kv_store")
  |> table.privacy(table.Public)
  |> table.write_concurrency(table.AutoWriteConcurrency)
  |> table.read_concurrency(True)
  |> table.decentralized_counters(True)
  |> table.compression(False)
  |> table.set()
}

pub fn get(set: Set(RespData, Entry), key: RespData) {
  let now = birl.utc_now()
  let entries = table.lookup(set, key)
  let #(valid, expired) = {
    use #(_, Entry(_, expiration)) <- list.partition(entries)
    case option.map(expiration, birl.compare(now, _)) {
      Some(Gt) | Some(Eq) -> False
      _ -> True
    }
  }

  expired
  |> list.map(pair.first)
  |> list.each(table.delete(set, _))

  valid
  |> list.map(fn(entry) { pair.second(entry).value })
  |> list.first
  |> result.unwrap(BulkString(None))
}

pub fn set(set: Set(RespData, Entry), key: RespData, entry: Entry) {
  [#(key, entry)]
  |> table.insert(set, _)
}

pub fn delete(set: Set(RespData, Entry), key: RespData) {
  table.delete(set, key)
}

pub fn dump(_set: Set(RespData, Entry)) {
  empty_rdb |> bit_array.base16_decode
}
