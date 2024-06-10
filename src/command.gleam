import birl
import birl/duration
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import types.{type Entry, type RespData, Array, BulkString, Entry}

pub type Command {
  Ping
  Echo(payload: RespData)
  Set(key: RespData, value: Entry)
  Get(key: RespData)
  Del(key: RespData)
  ReplConf(config_pairs: List(RespData))
  Psync(id: String, offset: Int)
}

pub type CommandParseError {
  InvalidCommand
  InvalidArgument(expected: String)
}

pub fn parse_all(commands: List(RespData)) {
  list.map(commands, parse_command)
}

pub fn parse_command(command: RespData) {
  case command {
    Array([BulkString(Some("PING"))]) -> Ok(Ping)
    Array([BulkString(Some("ECHO")), payload]) -> Ok(Echo(payload))
    Array([BulkString(Some("SET")), key, value]) ->
      Ok(Set(key, Entry(value, None)))
    Array([
      BulkString(Some("SET")),
      key,
      value,
      BulkString(Some("EX")),
      BulkString(Some(secs)),
    ]) -> {
      case int.parse(secs) {
        Ok(secs) -> {
          let now = birl.now()
          let duration = duration.seconds(secs)
          let expiry = birl.add(now, duration)
          Ok(Set(key, Entry(value, Some(expiry))))
        }
        Error(_) ->
          Error(InvalidArgument(
            "expected seconds to be an integer in a bulkstring",
          ))
      }
    }
    Array([
      BulkString(Some("SET")),
      key,
      value,
      BulkString(Some("PX")),
      BulkString(Some(milli)),
    ]) -> {
      case int.parse(milli) {
        Ok(milli) -> {
          let now = birl.now()
          let duration = duration.milli_seconds(milli)
          let expiry = birl.add(now, duration)
          Ok(Set(key, Entry(value, Some(expiry))))
        }
        Error(_) ->
          Error(InvalidArgument(
            "expected milliseconds to be an integer in a bulkstring",
          ))
      }
    }
    Array([BulkString(Some("GET")), key]) -> Ok(Get(key))
    Array([BulkString(Some("DEL")), key]) -> Ok(Del(key))
    Array([BulkString(Some("REPLCONF")), ..kv_pairs]) -> Ok(ReplConf(kv_pairs))
    Array([
      BulkString(Some("PSYNC")),
      BulkString(Some(id)),
      BulkString(Some(offset)),
    ]) -> {
      case int.parse(offset) {
        Ok(offset) -> Ok(Psync(id, offset))
        Error(_) ->
          Error(InvalidArgument(
            "expected offset to be an integer in a bulkstring",
          ))
      }
    }
    _ -> Error(InvalidCommand)
  }
}
