import bravo/uset.{type USet}
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
    kv_store: USet(#(RespData, Entry)),
    subject: Subject(Message(ConnMessage)),
  )
}
