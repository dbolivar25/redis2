import carpenter/table.{type Set}
import gleam/erlang/process.{type Subject}
import glisten/internal/handler.{type Message}
import types.{type Entry, type RespData}

pub type ConnMessage {
  Ack
  Forward(BitArray)
}

pub type Conn {
  Conn(
    buffer: BitArray,
    kv_store: Set(RespData, Entry),
    subject: Subject(Message(ConnMessage)),
  )
}
