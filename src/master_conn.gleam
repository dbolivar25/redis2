import bravo/uset.{type USet}
import codec
import command.{Del, Set}
import flash
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor.{Failed, Ready, Spec}
import glisten.{Packet}
import kv_store
import mug.{type Socket}
import types.{type Entry, type RespData, Array, BulkString}
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

pub fn new(host: String, port: Int, kv_store: USet(#(RespData, Entry))) {
  actor.start_spec(Spec(
    init: fn() {
      case
        mug.new(host, port)
        |> mug.connect()
      {
        Ok(socket) -> {
          let bits = codec.encode_resp_data(Array([BulkString(Some("PING"))]))
          let assert Ok(_) = mug.send(socket, bits)
          let assert Ok(_) = mug.receive(socket, 100)

          let bits =
            codec.encode_resp_data(
              Array([
                BulkString(Some("REPLCONF")),
                BulkString(Some("foo")),
                BulkString(Some("bar")),
              ]),
            )
          let assert Ok(_) = mug.send(socket, bits)
          let assert Ok(_) = mug.receive(socket, 100)

          let bits =
            codec.encode_resp_data(
              Array([
                BulkString(Some("REPLCONF")),
                BulkString(Some("bow")),
                BulkString(Some("baz")),
              ]),
            )
          let assert Ok(_) = mug.send(socket, bits)
          let assert Ok(_) = mug.receive(socket, 100)

          let bits =
            codec.encode_resp_data(
              Array([
                BulkString(Some("PSYNC")),
                BulkString(Some("?")),
                BulkString(Some("-1")),
              ]),
            )
          let assert Ok(_) = mug.send(socket, bits)
          let assert Ok(_) = mug.receive(socket, 100)

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
    init_timeout: 1000,
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
      io.debug(bits)
      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("server")
      |> flash.with_attr(flash.StringAttr("packet", utils.escape_ascii(bits)))
      |> flash.info("Packet received.")

      let buffer = <<state.buffer:bits, bits:bits>>

      case codec.parse_all(buffer) {
        Ok(#(resp_datas, rest)) -> {
          flash.new(flash.InfoLevel, flash.text_writer)
          |> flash.with_group("server")
          |> flash.info("RESP data parsed.")

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

          actor.continue(MasterConn(..state, buffer: rest))
        }
        Error(_) -> {
          flash.new(flash.InfoLevel, flash.text_writer)
          |> flash.with_group("server")
          |> flash.info("Connection closed.")

          // actor.send(conn_manager, RmConn(state.subject))
          actor.Stop(process.Normal)
        }
      }
    }
  }
}
