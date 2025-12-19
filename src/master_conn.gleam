import bravo/uset.{type USet}
import codec
import command.{Del, Set}
import flash
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor.{Failed, Ready, Spec}
import gleam/string
import kv_store
import mug.{type Socket}
import types.{type Entry, type RespData, Array, BulkString, SimpleString}
import utils

pub type MasterConnMessage {
  Shutdown
  Packet(BitArray)
}

pub type MasterConn {
  MasterConn(
    socket: Socket,
    buffer: BitArray,
    kv_store: USet(#(RespData, Entry)),
  )
}

fn log(msg: String) {
  flash.new(flash.InfoLevel, flash.text_writer)
  |> flash.with_group("replica")
  |> flash.info(msg)
}

fn log_with_attr(msg: String, key: String, value: String) {
  flash.new(flash.InfoLevel, flash.text_writer)
  |> flash.with_group("replica")
  |> flash.with_attr(flash.StringAttr(key, value))
  |> flash.info(msg)
}

/// Receives FULLRESYNC response and snapshot, handling partial TCP receives
fn receive_fullresync_and_snapshot(
  socket: Socket,
  initial_bits: BitArray,
  kv: USet(#(RespData, Entry)),
) -> Result(Nil, String) {
  receive_and_parse_loop(socket, initial_bits, kv, 10)
}

fn receive_and_parse_loop(
  socket: Socket,
  buffer: BitArray,
  kv: USet(#(RespData, Entry)),
  retries: Int,
) -> Result(Nil, String) {
  case retries {
    0 -> Error("Timeout waiting for complete PSYNC response")
    _ -> {
      case codec.parse_all(buffer) {
        // Got FULLRESYNC + snapshot
        Ok(#([SimpleString(fullresync), snapshot_data], _rest)) -> {
          case string.starts_with(fullresync, "FULLRESYNC") {
            True -> {
              log_with_attr("Received FULLRESYNC.", "response", fullresync)
              case codec.decode_snapshot(snapshot_data) {
                Ok(entries) -> {
                  log_with_attr(
                    "Loading keys from master.",
                    "count",
                    int.to_string(list.length(entries)),
                  )
                  kv_store.load_snapshot(kv, entries)
                  Ok(Nil)
                }
                Error(err) -> Error("Failed to decode snapshot: " <> err)
              }
            }
            False -> Error("Expected FULLRESYNC, got: " <> fullresync)
          }
        }
        // Got just one SimpleString - could be FULLRESYNC (need snapshot) or CONTINUE
        Ok(#([SimpleString(msg)], rest)) -> {
          case string.starts_with(msg, "FULLRESYNC") {
            True -> {
              log_with_attr("Received FULLRESYNC.", "response", msg)
              // Need to receive more data for snapshot
              case mug.receive(socket, 5000) {
                Ok(more_bits) -> {
                  let new_buffer = <<rest:bits, more_bits:bits>>
                  receive_snapshot_loop(socket, new_buffer, kv, 10)
                }
                Error(_) -> Error("Failed to receive snapshot data")
              }
            }
            False -> {
              case msg {
                "CONTINUE" -> {
                  log("Received CONTINUE (partial sync).")
                  Ok(Nil)
                }
                _ -> Error("Unexpected response: " <> msg)
              }
            }
          }
        }
        // Incomplete data, receive more
        Ok(#([], _rest)) -> {
          case mug.receive(socket, 1000) {
            Ok(more_bits) -> {
              let new_buffer = <<buffer:bits, more_bits:bits>>
              receive_and_parse_loop(socket, new_buffer, kv, retries - 1)
            }
            Error(_) -> Error("Failed to receive more data")
          }
        }
        Ok(#(other, _rest)) -> {
          Error("Unexpected response format: " <> string.inspect(other))
        }
        Error(_) -> {
          // Parse error - might need more data
          case mug.receive(socket, 1000) {
            Ok(more_bits) -> {
              let new_buffer = <<buffer:bits, more_bits:bits>>
              receive_and_parse_loop(socket, new_buffer, kv, retries - 1)
            }
            Error(_) -> Error("Failed to parse PSYNC response")
          }
        }
      }
    }
  }
}

fn receive_snapshot_loop(
  socket: Socket,
  buffer: BitArray,
  kv: USet(#(RespData, Entry)),
  retries: Int,
) -> Result(Nil, String) {
  case retries {
    0 -> Error("Timeout waiting for snapshot")
    _ -> {
      case codec.parse_all(buffer) {
        Ok(#([snapshot_data], _rest)) -> {
          case codec.decode_snapshot(snapshot_data) {
            Ok(entries) -> {
              log_with_attr(
                "Loading keys from master.",
                "count",
                int.to_string(list.length(entries)),
              )
              kv_store.load_snapshot(kv, entries)
              Ok(Nil)
            }
            Error(err) -> Error("Failed to decode snapshot: " <> err)
          }
        }
        Ok(#([], _rest)) -> {
          // Need more data
          case mug.receive(socket, 1000) {
            Ok(more_bits) -> {
              let new_buffer = <<buffer:bits, more_bits:bits>>
              receive_snapshot_loop(socket, new_buffer, kv, retries - 1)
            }
            Error(_) -> Error("Failed to receive snapshot data")
          }
        }
        Ok(#(other, _rest)) -> {
          Error("Unexpected snapshot format: " <> string.inspect(other))
        }
        Error(_) -> {
          case mug.receive(socket, 1000) {
            Ok(more_bits) -> {
              let new_buffer = <<buffer:bits, more_bits:bits>>
              receive_snapshot_loop(socket, new_buffer, kv, retries - 1)
            }
            Error(_) -> Error("Failed to parse snapshot")
          }
        }
      }
    }
  }
}

pub fn new(host: String, port: Int, kv_store: USet(#(RespData, Entry))) {
  actor.start_spec(Spec(
    init: fn() {
      case
        mug.new(host, port)
        |> mug.connect()
      {
        Ok(socket) -> {
          log_with_attr(
            "Connected to master.",
            "host",
            host <> ":" <> int.to_string(port),
          )

          // Send PING
          let bits = codec.encode_resp_data(Array([BulkString(Some("PING"))]))
          let assert Ok(_) = mug.send(socket, bits)
          let assert Ok(_) = mug.receive(socket, 5000)

          // Send PSYNC ? -1 for full sync
          let bits =
            codec.encode_resp_data(
              Array([
                BulkString(Some("PSYNC")),
                BulkString(Some("?")),
                BulkString(Some("-1")),
              ]),
            )
          let assert Ok(_) = mug.send(socket, bits)

          // Receive FULLRESYNC response and snapshot
          let assert Ok(initial_bits) = mug.receive(socket, 5000)
          log_with_attr(
            "Received initial response.",
            "bytes",
            int.to_string(bit_array.byte_size(initial_bits)),
          )

          case receive_fullresync_and_snapshot(socket, initial_bits, kv_store) {
            Ok(_) -> log("Full sync complete.")
            Error(err) -> log_with_attr("Full sync failed.", "error", err)
          }

          mug.receive_next_packet_as_message(socket)
          process.new_selector()
          |> mug.selecting_tcp_messages(fn(tcp_msg) {
            case tcp_msg {
              mug.Packet(_, bits) -> {
                Packet(bits)
              }
              _ -> {
                Shutdown
              }
            }
          })
          |> Ready(MasterConn(socket, <<>>, kv_store), _)
        }
        Error(_) -> {
          Failed("Failed to connect to the master instance.")
        }
      }
    },
    init_timeout: 30_000,
    loop: on_msg_fn(),
  ))
}

fn on_msg_fn() {
  msg_handler
}

fn msg_handler(msg: MasterConnMessage, state: MasterConn) {
  case msg {
    Shutdown -> {
      actor.Stop(process.Normal)
    }
    Packet(bits) -> {
      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("replica")
      |> flash.with_attr(flash.StringAttr("packet", utils.escape_ascii(bits)))
      |> flash.info("Packet received from master.")

      let buffer = <<state.buffer:bits, bits:bits>>

      case codec.parse_all(buffer) {
        Ok(#(resp_datas, rest)) -> {
          command.parse_all(resp_datas)
          |> list.each(fn(result) {
            case result {
              Ok(command) ->
                case command {
                  Set(key, value) -> {
                    kv_store.set(state.kv_store, key, value)
                    Nil
                  }
                  Del(key) -> {
                    kv_store.delete(state.kv_store, key)
                  }
                  _ -> panic as "Forwarded commands can only be set or del"
                }
              Error(_err) -> {
                Nil
              }
            }
          })

          // Request the next packet from master
          mug.receive_next_packet_as_message(state.socket)
          actor.continue(MasterConn(..state, buffer: rest))
        }
        Error(_) -> {
          log("Connection to master closed.")
          actor.Stop(process.Normal)
        }
      }
    }
  }
}
