import gleam/int
import gleam/list
import gleam/result
import gleam/string

// const empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

pub fn n_eq(a, b) {
  a != b
}

pub fn unwrap(res: Result(a, b)) -> a {
  result.lazy_unwrap(res, fn() { panic })
}

pub fn expect(res: Result(a, b), str: String) -> a {
  result.lazy_unwrap(res, fn() {
    panic as string.append("expectation failed: ", str)
  })
}

@external(erlang, "erlang", "binary_to_list")
fn binary_to_bytes(bin: BitArray) -> List(Int)

pub fn escape_ascii(input: BitArray) -> String {
  binary_to_bytes(input)
  |> list.map(escape_byte)
  |> string.join("")
}

fn escape_byte(byte: Int) -> String {
  case <<byte:int>> {
    <<"\t":utf8>> -> "\\t"
    <<"\r":utf8>> -> "\\r"
    <<"\n":utf8>> -> "\\n"
    <<"'":utf8>> -> "\\'"
    <<"\"":utf8>> -> "\\\""
    <<"\\":utf8>> -> "\\\\"
    _ if byte >= 0x20 && byte <= 0x7e ->
      string.utf_codepoint(byte)
      |> expect("byte is valid utf8")
      |> list.prepend([], _)
      |> string.from_utf_codepoints
    _ -> hex_escape(byte)
  }
}

fn hex_escape(byte: Int) -> String {
  int.to_base16(byte)
  |> string.lowercase()
  |> string.pad_left(2, "0")
  |> string.append("\\x", _)
}
