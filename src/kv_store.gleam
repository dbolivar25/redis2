import birl.{type Time}
import carpenter/table.{type Set}
import gleam/list
import gleam/option.{type Option, Some}
import gleam/order.{Eq, Gt}
import gleam/pair
import types.{type Entry, type RespData, Entry}

pub fn new() {
  table.build("kv_store")
  |> table.privacy(table.Private)
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
}

pub fn set(
  set: Set(RespData, Entry),
  key: RespData,
  value: RespData,
  expiration: Option(Time),
) {
  [#(key, Entry(value, expiration))]
  |> table.insert(set, _)
}

pub fn delete(set: Set(RespData, Entry), key: RespData) {
  table.delete(set, key)
}
