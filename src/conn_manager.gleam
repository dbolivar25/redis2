import conn.{type ConnMessage, Forward}
import flash
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import glisten/internal/handler.{type Message}
import utils.{n_eq}

pub type ConnManagerMessage {
  AddConn(Subject(Message(ConnMessage)))
  RmConn(Subject(Message(ConnMessage)))
  SetClient(Subject(Message(ConnMessage)))
  SetReplica(Subject(Message(ConnMessage)))
  SendToReplicas(BitArray)
}

pub type ConnManager {
  ConnManager(
    // master: Option(Subject(Message(MasterConnMessage))),
    clients: List(Subject(Message(ConnMessage))),
    replicas: List(Subject(Message(ConnMessage))),
  )
}

pub fn new() {
  actor.start(ConnManager([], []), on_msg_fn())
}

pub fn add_conn(
  actor: Subject(ConnManagerMessage),
  conn: Subject(Message(ConnMessage)),
) {
  actor.send(actor, AddConn(conn))
}

pub fn rm_conn(
  actor: Subject(ConnManagerMessage),
  conn: Subject(Message(ConnMessage)),
) {
  actor.send(actor, RmConn(conn))
}

pub fn set_client(
  actor: Subject(ConnManagerMessage),
  conn: Subject(Message(ConnMessage)),
) {
  actor.send(actor, SetClient(conn))
}

pub fn set_replica(
  actor: Subject(ConnManagerMessage),
  conn: Subject(Message(ConnMessage)),
) {
  actor.send(actor, SetReplica(conn))
}

pub fn send_to_replicas(actor: Subject(ConnManagerMessage), msg: BitArray) {
  actor.send(actor, SendToReplicas(msg))
}

fn on_msg_fn() {
  msg_handler
}

fn msg_handler(msg: ConnManagerMessage, state: ConnManager) {
  case msg {
    AddConn(conn) -> {
      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("conn_manager")
      |> flash.info("Adding connection.")

      actor.continue(ConnManager(..state, clients: [conn, ..state.clients]))
    }
    RmConn(conn) -> {
      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("conn_manager")
      |> flash.info("Removing connection.")

      actor.continue(ConnManager(
        clients: list.filter(state.clients, n_eq(_, conn)),
        replicas: list.filter(state.replicas, n_eq(_, conn)),
      ))
    }
    SetClient(conn) -> {
      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("conn_manager")
      |> flash.info("Setting connection as client.")

      actor.continue(ConnManager(
        clients: [conn, ..state.clients],
        replicas: list.filter(state.replicas, n_eq(_, conn)),
      ))
    }
    SetReplica(conn) -> {
      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("conn_manager")
      |> flash.info("Setting connection as replica.")

      actor.continue(
        ConnManager(
          clients: list.filter(state.clients, n_eq(_, conn)),
          replicas: [conn, ..state.replicas],
        ),
      )
    }
    SendToReplicas(msg) -> {
      flash.new(flash.InfoLevel, flash.text_writer)
      |> flash.with_group("conn_manager")
      |> flash.info("Sending message to replicas.")

      state.replicas
      |> list.each(actor.send(_, handler.User(Forward(msg))))

      actor.continue(state)
    }
  }
}
