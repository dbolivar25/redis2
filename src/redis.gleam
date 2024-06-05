import flash
import gleam/erlang/process
import gleam/int

import conn_manager
import kv_store
import server

pub fn main() {
  let port = 6379

  let assert Ok(kv_store) = kv_store.new()
  let assert Ok(conn_manager) = conn_manager.new()
  let assert Ok(_) = server.new(kv_store, conn_manager) |> server.serve(port)

  flash.new(flash.InfoLevel, flash.text_writer)
  |> flash.with_group("main")
  |> flash.info("Listening on port " <> int.to_string(port) <> ".")

  process.sleep_forever()
}
