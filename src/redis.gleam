import argv
import flash
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/pair
import gleam/result
import gleam/string

import conn_manager
import kv_store
import master_conn
import server

const default_port = 6379

pub fn main() {
  case argv.load().arguments {
    ["-p", port, "-r", master]
    | ["--port", port, "--replica_of", master]
    | ["-r", master, "-p", port]
    | ["--replica_of", master, "--port", port] -> {
      case
        int.parse(port),
        string.split_once(master, ":")
        |> result.map(pair.map_second(_, int.parse))
      {
        Ok(port), Ok(#(host, Ok(host_port))) -> {
          let assert Ok(kv_store) = kv_store.new()
          let assert Ok(_) = master_conn.new(host, host_port, kv_store)
          let assert Ok(conn_manager) = conn_manager.new()
          let assert Ok(_) =
            server.new(kv_store, conn_manager) |> server.serve(port)

          flash.new(flash.InfoLevel, flash.text_writer)
          |> flash.with_group("main")
          |> flash.info("Listening on port " <> int.to_string(port) <> ".")

          process.sleep_forever()
        }
        Error(_), Ok(_) -> {
          io.println_error("Invalid port number.")
        }
        Ok(_), Error(_) -> {
          io.println_error("Invalid host address.")
        }
        _, _ -> {
          io.println_error("Invalid port number and host address.")
        }
      }
    }
    ["-p", port] | ["--port", port] -> {
      case int.parse(port) {
        Ok(port) -> {
          let assert Ok(kv_store) = kv_store.new()
          let assert Ok(conn_manager) = conn_manager.new()
          let assert Ok(_) =
            server.new(kv_store, conn_manager) |> server.serve(port)

          flash.new(flash.InfoLevel, flash.text_writer)
          |> flash.with_group("main")
          |> flash.info("Listening on port " <> int.to_string(port) <> ".")

          process.sleep_forever()
        }
        Error(_) -> {
          io.println_error("Invalid port number.")
        }
      }
    }
    [] -> {
      let assert Ok(kv_store) = kv_store.new()
      let assert Ok(conn_manager) = conn_manager.new()
      let assert Ok(_) =
        server.new(kv_store, conn_manager) |> server.serve(default_port)

      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("main")
      |> flash.info("Listening on port " <> int.to_string(default_port) <> ".")

      process.sleep_forever()
    }
    ["-h"] | ["--help"] | _ -> {
      io.println("Redis Server\n")
      io.println("Usage: redis2 [OPTIONS]\n")
      io.println("Options:")
      io.println(
        "  -p, --port <PORT>           Port to listen on. Default is 6379.",
      )
      io.println(
        "  -r, --replica_of <MASTER>   Make the server a replica of another instance.",
      )
      io.println(
        "  -h, --help                  Show this help message and exit.",
      )
    }
  }
}
