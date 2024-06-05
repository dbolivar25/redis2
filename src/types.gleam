import birl.{type Time}
import gleam/option.{type Option}

pub type RespData {
  SimpleString(String)
  SimpleError(String)
  Integer(Int)
  BulkString(Option(String))
  Array(List(RespData))
  RDBFile(BitArray)
  Null
}

pub type Entry {
  Entry(value: RespData, expiration: Option(Time))
}
