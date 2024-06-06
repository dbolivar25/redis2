import gleam/bit_array
import gleam/bool
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import types.{
  type RespData, Array, BulkString, Integer, Null, RDBFile, SimpleError,
  SimpleString,
}

pub type RespParseError {
  Incomplete
  InvalidUtf
  InvalidFormat
}

pub fn encode_resp_data(data: RespData) {
  case data {
    SimpleString(value) -> encode_simple_string(value)
    SimpleError(value) -> encode_simple_error(value)
    Integer(value) -> encode_integer(value)
    BulkString(value) -> encode_bulk_string(value)
    Array(value) -> encode_array(value)
    RDBFile(value) -> encode_rdb_file(value)
    Null -> <<"_\r\n":utf8>>
  }
}

fn encode_simple_string(value: String) {
  <<"+":utf8, value:utf8, "\r\n":utf8>>
}

fn encode_simple_error(value: String) {
  <<"-":utf8, value:utf8, "\r\n":utf8>>
}

fn encode_integer(value: Int) {
  let value_str = int.to_string(value)
  <<":":utf8, value_str:utf8, "\r\n":utf8>>
}

fn encode_bulk_string(value: Option(String)) {
  case value {
    None -> <<"$-1\r\n":utf8>>
    Some(value) -> {
      let len = string.length(value) |> int.to_string
      <<"$":utf8, len:utf8, "\r\n":utf8, value:utf8, "\r\n":utf8>>
    }
  }
}

fn encode_array(value: List(RespData)) {
  let len = list.length(value) |> int.to_string
  let encoded =
    list.fold(value, <<>>, fn(acc, x) { <<acc:bits, encode_resp_data(x):bits>> })
  <<"*":utf8, len:utf8, "\r\n":utf8, encoded:bits>>
}

fn encode_rdb_file(value: BitArray) {
  let len = bit_array.byte_size(value) |> int.to_string
  <<"$":utf8, len:utf8, "\r\n":utf8, value:bits>>
}

pub fn parse_all(data: BitArray) {
  parse_all_impl(data, [])
}

fn parse_all_impl(data: BitArray, acc: List(RespData)) {
  case parse_resp_data(data) {
    Ok(#(parsed, rest)) -> {
      case rest {
        <<>> -> Ok(#(list.reverse([parsed, ..acc]), rest))
        _ -> parse_all_impl(rest, [parsed, ..acc])
      }
    }
    Error(Incomplete) -> Ok(#(list.reverse(acc), data))
    Error(err) -> Error(#(err, data))
  }
}

pub fn parse_resp_data(data: BitArray) {
  case data {
    <<"$-1\r\n":utf8, rest:bits>> -> Ok(#(BulkString(None), rest))
    <<"*-1\r\n":utf8, rest:bits>> -> Ok(#(Array([]), rest))
    <<"_\r\n":utf8, rest:bits>> -> Ok(#(Null, rest))
    <<"+":utf8, rest:bits>> -> parse_simple_string(rest)
    <<"-":utf8, rest:bits>> -> parse_simple_error(rest)
    <<":":utf8, rest:bits>> -> parse_integer(rest)
    <<"$":utf8, rest:bits>> -> parse_bulk_string(rest)
    <<"*":utf8, rest:bits>> -> parse_array(rest)
    _ -> Error(InvalidFormat)
  }
}

fn parse_simple_string(data: BitArray) {
  parse_simple_string_impl(data, <<>>)
}

fn parse_simple_error(data: BitArray) {
  parse_simple_error_impl(data, <<>>)
}

fn parse_integer(data: BitArray) {
  use #(len, rest) <- result.try(parse_raw_int(data, 0))
  Ok(#(Integer(len), rest))
}

fn parse_bulk_string(data: BitArray) {
  use #(len, rest) <- result.try(parse_raw_int(data, 0))
  parse_bulk_string_impl(rest, len)
}

fn parse_array(data: BitArray) {
  use #(len, rest) <- result.try(parse_raw_int(data, 0))
  parse_array_impl(rest, [], len)
}

pub fn parse_rdb_file(data: BitArray) {
  case data {
    <<"$":utf8, payload:bits>> -> {
      use #(len, bin) <- result.try(parse_raw_int(payload, 0))
      use <- bool.guard(bit_array.byte_size(bin) < len, Error(Incomplete))
      case bin {
        <<selected:bytes-size(len), rest:bits>> ->
          Ok(#(RDBFile(selected), rest))
        _ -> Error(InvalidFormat)
      }
    }
    _ -> Error(InvalidFormat)
  }
}

fn parse_simple_string_impl(data: BitArray, acc: BitArray) {
  case data {
    <<"\r":utf8>> | <<>> -> Error(Incomplete)
    <<"\r":utf8, _:bits>> -> Error(InvalidFormat)
    <<"\r\n":utf8, rest:bits>> ->
      case bit_array.to_string(acc) {
        Error(_) -> Error(InvalidUtf)
        Ok(parsed) -> Ok(#(SimpleString(parsed), rest))
      }
    <<byte, rest:bits>> -> parse_simple_string_impl(rest, <<acc:bits, byte>>)
    _ -> Error(InvalidFormat)
  }
}

fn parse_simple_error_impl(data: BitArray, acc: BitArray) {
  case data {
    <<"\r":utf8>> | <<>> -> Error(Incomplete)
    <<"\r\n":utf8, rest:bits>> ->
      case bit_array.to_string(acc) {
        Error(_) -> Error(InvalidUtf)
        Ok(parsed) -> Ok(#(SimpleError(parsed), rest))
      }
    <<byte, rest:bits>> -> parse_simple_error_impl(rest, <<acc:bits, byte>>)
    _ -> Error(InvalidFormat)
  }
}

fn parse_bulk_string_impl(data: BitArray, len: Int) {
  case data {
    <<>> -> Error(Incomplete)
    <<_:bytes-size(len)>> -> Error(Incomplete)
    <<_:bytes-size(len), "\r":utf8>> -> Error(Incomplete)
    <<selected:bytes-size(len), "\r\n":utf8, rest:bits>> ->
      case bit_array.to_string(selected) {
        Error(_) -> Error(InvalidUtf)
        Ok(parsed) -> Ok(#(BulkString(Some(parsed)), rest))
      }
    _ -> Error(InvalidFormat)
  }
}

fn parse_array_impl(data: BitArray, acc: List(RespData), len: Int) {
  case len {
    0 -> Ok(#(Array(list.reverse(acc)), data))
    _ -> {
      use #(parsed, rest) <- result.try(parse_resp_data(data))
      parse_array_impl(rest, [parsed, ..acc], len - 1)
    }
  }
}

fn parse_raw_int(input: BitArray, acc: Int) {
  case input {
    <<"\r":utf8>> | <<>> -> Error(Incomplete)
    <<"0":utf8, rest:bits>> -> parse_raw_int(rest, acc * 10)
    <<"1":utf8, rest:bits>> -> parse_raw_int(rest, 1 + acc * 10)
    <<"2":utf8, rest:bits>> -> parse_raw_int(rest, 2 + acc * 10)
    <<"3":utf8, rest:bits>> -> parse_raw_int(rest, 3 + acc * 10)
    <<"4":utf8, rest:bits>> -> parse_raw_int(rest, 4 + acc * 10)
    <<"5":utf8, rest:bits>> -> parse_raw_int(rest, 5 + acc * 10)
    <<"6":utf8, rest:bits>> -> parse_raw_int(rest, 6 + acc * 10)
    <<"7":utf8, rest:bits>> -> parse_raw_int(rest, 7 + acc * 10)
    <<"8":utf8, rest:bits>> -> parse_raw_int(rest, 8 + acc * 10)
    <<"9":utf8, rest:bits>> -> parse_raw_int(rest, 9 + acc * 10)
    <<"\r\n":utf8, rest:bits>> -> Ok(#(acc, rest))
    _ -> Error(InvalidFormat)
  }
}
