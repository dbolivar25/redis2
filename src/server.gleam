import carpenter/table.{type Set}
import codec.{Incomplete}
import conn.{type Conn, type ConnMessage, Ack, Conn, Forward}
import conn_manager.{type ConnManagerMessage, AddConn, RmConn}
import flash
import gleam/bytes_builder
import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import glisten.{type Connection, type Handler, type Message, Packet, User}
import types.{type Entry, type RespData}
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
  _kv_store: Set(RespData, Entry),
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

        case codec.parse_resp_data(buffer) {
          Ok(#(parsed, rest)) -> {
            flash.new(flash.InfoLevel, flash.text_writer)
            |> flash.with_group("server")
            |> flash.info("RESP data parsed.")
            io.debug(parsed)

            let resp = bytes_builder.from_bit_array(buffer)
            let _ = glisten.send(conn, resp)

            actor.continue(Conn(..state, buffer: rest))
          }
          Error(Incomplete) -> {
            flash.new(flash.InfoLevel, flash.text_writer)
            |> flash.with_group("server")
            |> flash.info("Incomplete packet.")

            actor.continue(Conn(..state, buffer: buffer))
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
