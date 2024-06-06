import carpenter/table.{type Set}
import codec.{Incomplete}
import command.{
  type Command, Del, Echo, Get, InvalidArgument, InvalidCommand, Ping, Psync,
  ReplConf, Set,
}
import conn.{type Conn, type ConnMessage, Ack, Conn, Forward}
import conn_manager.{type ConnManagerMessage, AddConn, RmConn}
import flash
import gleam/bytes_builder
import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/list
import gleam/option.{None}
import gleam/otp/actor
import glisten.{type Connection, type Handler, type Message, Packet, User}
import kv_store
import types.{
  type Entry, type RespData, Array, BulkString, Integer, Null, SimpleError,
  SimpleString,
}
import utils

pub fn new(
  kv_store: Set(RespData, Entry),
  conn_manager: Subject(ConnManagerMessage),
) {
  glisten.handler(
    on_init_fn(kv_store, conn_manager),
    on_msg_fn(kv_store, conn_manager),
  )
  |> glisten.with_close(on_close_fn(conn_manager))
}

pub fn serve(handler: Handler(ConnMessage, Conn), port: Int) {
  glisten.serve(handler, port)
}

fn on_init_fn(
  kv_store: Set(RespData, Entry),
  conn_manager: Subject(ConnManagerMessage),
) {
  fn(conn: Connection(ConnMessage)) {
    flash.new(flash.InfoLevel, flash.text_writer)
    |> flash.with_group("server")
    |> flash.info("Connection established.")

    actor.send(conn_manager, AddConn(conn.subject))
    #(Conn(<<>>, kv_store, conn.subject), None)
  }
}

fn on_msg_fn(
  kv_store: Set(RespData, Entry),
  conn_manager: Subject(ConnManagerMessage),
) {
  fn(msg: Message(ConnMessage), state: Conn, conn: Connection(ConnMessage)) {
    case msg {
      Packet(bits) -> {
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
                    Ping -> {
                      let encoded = codec.encode_resp_data(SimpleString("PONG"))
                      let bytes = bytes_builder.from_bit_array(encoded)
                      let _ = glisten.send(conn, bytes)
                    }
                    Echo(msg) -> {
                      let encoded = codec.encode_resp_data(msg)
                      let bytes = bytes_builder.from_bit_array(encoded)
                      let _ = glisten.send(conn, bytes)
                    }
                    Set(key, value) -> {
                      conn_manager.send_to_replicas(conn_manager, buffer)
                      kv_store.set(kv_store, key, value)
                      let encoded = codec.encode_resp_data(SimpleString("OK"))
                      let bytes = bytes_builder.from_bit_array(encoded)
                      let _ = glisten.send(conn, bytes)
                    }
                    Get(key) -> {
                      let value = kv_store.get(kv_store, key)
                      let encoded = codec.encode_resp_data(value)
                      let bytes = bytes_builder.from_bit_array(encoded)
                      let _ = glisten.send(conn, bytes)
                    }
                    Del(key) -> {
                      conn_manager.send_to_replicas(conn_manager, buffer)
                      kv_store.delete(kv_store, key)
                      let encoded = codec.encode_resp_data(SimpleString("OK"))
                      let bytes = bytes_builder.from_bit_array(encoded)
                      let _ = glisten.send(conn, bytes)
                    }
                    ReplConf(_pairs) -> {
                      let encoded = codec.encode_resp_data(SimpleString("OK"))
                      let bytes = bytes_builder.from_bit_array(encoded)
                      let _ = glisten.send(conn, bytes)
                    }
                    Psync(id, offset) ->
                      case id, offset {
                        "?", -1 -> {
                          conn_manager.set_replica(conn_manager, conn.subject)
                          let encoded =
                            codec.encode_resp_data(SimpleString(
                              "FULLRESYNC master 0",
                            ))
                          let encoded_rdb_file =
                            kv_store.dump(kv_store)
                            |> utils.expect("rdb file is valid")
                            |> types.RDBFile
                            |> codec.encode_resp_data
                          let bytes =
                            bytes_builder.from_bit_array(encoded)
                            |> bytes_builder.append(encoded_rdb_file)
                          let _ = glisten.send(conn, bytes)
                        }
                        _, _ -> {
                          let encoded =
                            codec.encode_resp_data(SimpleString("CONTINUE"))
                          let bytes = bytes_builder.from_bit_array(encoded)
                          let _ = glisten.send(conn, bytes)
                        }
                      }
                  }
                Error(err) -> {
                  let encoded = case err {
                    InvalidArgument(msg) ->
                      codec.encode_resp_data(SimpleError(msg))
                    InvalidCommand ->
                      codec.encode_resp_data(SimpleError("unknown command"))
                  }
                  let bytes = bytes_builder.from_bit_array(encoded)
                  let _ = glisten.send(conn, bytes)
                }
              }
            })

            actor.continue(Conn(..state, buffer: rest))
          }
          Error(_) -> {
            flash.new(flash.InfoLevel, flash.text_writer)
            |> flash.with_group("server")
            |> flash.info("Connection closed.")

            actor.send(conn_manager, RmConn(state.subject))
            actor.Stop(process.Normal)
          }
        }
      }
      User(Forward(bits)) -> {
        flash.new(flash.InfoLevel, flash.text_writer)
        |> flash.with_group("server")
        |> flash.with_attr(flash.StringAttr("message", utils.escape_ascii(bits)))
        |> flash.info("Forward message received.")

        let resp = bytes_builder.from_bit_array(bits)
        let _ = glisten.send(conn, resp)

        actor.continue(state)
      }
      User(Ack) -> {
        flash.new(flash.InfoLevel, flash.text_writer)
        |> flash.with_group("server")
        |> flash.info("Ack message received.")

        actor.continue(state)
      }
    }
  }
}

fn on_close_fn(conn_manager: Subject(ConnManagerMessage)) {
  fn(state: Conn) {
    flash.new(flash.InfoLevel, flash.text_writer)
    |> flash.with_group("server")
    |> flash.info("Connection closed.")

    actor.send(conn_manager, RmConn(state.subject))
  }
}
