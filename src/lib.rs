#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{self, Write, Read};
use std::net::SocketAddr;
use std::sync::mpsc::{self, channel, Sender, Receiver};
use std::thread::{self, JoinHandle};

use mio::net::{TcpListener, TcpStream};
use mio::{Poll, Events, Token, Interest, Registry, Waker};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub trait UserMessage: Send + Serialize + DeserializeOwned + 'static {}
impl<T: Send + Serialize + DeserializeOwned + 'static> UserMessage for T {}

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    BincodeError(bincode::Error),
    ChannelSendError,
    ChannelRecvError,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IoError(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::BincodeError(err)
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(_: mpsc::SendError<T>) -> Self {
        Error::ChannelSendError
    }
}

impl<T> From<mpsc::TrySendError<T>> for Error {
    fn from(_: mpsc::TrySendError<T>) -> Self {
        Error::ChannelSendError
    }
}

impl From<mpsc::RecvError> for Error {
    fn from(_: mpsc::RecvError) -> Self {
        Error::ChannelRecvError
    }
}

impl From<mpsc::TryRecvError> for Error {
    fn from(_: mpsc::TryRecvError) -> Self {
        Error::ChannelRecvError
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct ConnId(Token);

pub struct ServerMessage<TMessage: UserMessage> {
    token: ConnId,
    message: ServerMessagePayload<TMessage>,
}

impl<TMessage: UserMessage> ServerMessage<TMessage> {
    fn new(token: ConnId, message: ServerMessagePayload<TMessage>) -> Self {
        Self { token, message }
    }
}

pub enum ServerMessagePayload<TMessage: UserMessage> {
    ClientConnected,
    ClientDisconnected,
    Message(TMessage),
}

pub struct Server<TMessage: UserMessage> {
    incoming_messages: Receiver<ServerMessage<TMessage>>,
    outgoing_messages: Sender<(ConnId, TMessage)>,

    poll_waker: Waker,

    thread_join_handle: JoinHandle<Result<()>>,
}

impl<TMessage: UserMessage> Server<TMessage> {
    pub fn get_next_message(&mut self) -> Result<Option<ServerMessage<TMessage>>> {
        match self.incoming_messages.try_recv() {
            Ok(m) => Ok(Some(m)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(e) => Err(e)?
        }
    }

    pub fn send_message(&mut self, msg_target: ConnId, msg: TMessage) -> Result<()> {
        self.outgoing_messages.send((msg_target, msg))?;
        self.poll_waker.wake()?;

        Ok(())
    }
}

type MessageSize = u16;
const MESSAGE_SIZE_BYTES: usize = std::mem::size_of::<MessageSize>();

enum ConnectionReadState {
    ReadingMessageSize,
    ReadingMessage(MessageSize),
}

struct Connection<TMessage: UserMessage> {
    socket: TcpStream,
    token: Token,

    read_state: ConnectionReadState,
    read_buffer: Vec<u8>,
    read_buffer_received: usize,

    messages_to_write: Vec<TMessage>,
    currently_writing_message: Option<TMessage>,
    write_buffer: Vec<u8>,
    write_buffer_sent: usize,
}

impl<TMessage: UserMessage> Connection<TMessage> {
    fn new(socket: TcpStream, token: Token) -> Self {
        Self {
            socket,
            token,

            read_state: ConnectionReadState::ReadingMessageSize,
            read_buffer: vec![0u8; 32768],
            read_buffer_received: 0,

            messages_to_write: vec![],
            currently_writing_message: None,
            write_buffer: vec![0u8, 0],
            write_buffer_sent: 0,
        }
    }

    fn try_setup_writing_next_message(&mut self) -> Result<()> {
        if self.currently_writing_message.is_some() || self.messages_to_write.len() == 0 {
            return Ok(());
        }

        let msg = self.messages_to_write.remove(0);
        let msg_bytes = bincode::serialize(&msg)?;
        let size = (msg_bytes.len() as u16).to_be_bytes();

        self.write_buffer = Vec::with_capacity(msg_bytes.len() + size.len());
        self.write_buffer.extend_from_slice(&size);
        self.write_buffer.extend_from_slice(&msg_bytes);

        self.write_buffer_sent = 0;
        self.currently_writing_message = Some(msg);

        Ok(())
    }

    fn try_do_reading(&mut self, message_channel: &mut Sender<ServerMessage<TMessage>>) -> Result<bool> {
        loop {
            match self.socket.read(&mut self.read_buffer[self.read_buffer_received..]) {
                Ok(0) => return Ok(true),
                Ok(n) => {
                    self.read_buffer_received += n;

                    loop {
                        match self.read_state {
                            ConnectionReadState::ReadingMessageSize => {
                                if self.read_buffer_received < MESSAGE_SIZE_BYTES {
                                    break;
                                }

                                let size = MessageSize::from_be_bytes(
                                    self.read_buffer[..MESSAGE_SIZE_BYTES]
                                        .try_into()
                                        .expect("2 bytes is not 2 bytes")
                                );
                                
                                self.read_buffer.rotate_left(MESSAGE_SIZE_BYTES);
                                self.read_buffer_received -= MESSAGE_SIZE_BYTES;
                                self.read_state = ConnectionReadState::ReadingMessage(size);
                            },

                            ConnectionReadState::ReadingMessage(size) => {
                                let size = size as usize;
                                if self.read_buffer_received < size {
                                    break;
                                }

                                let msg = bincode::deserialize(&self.read_buffer[..size])?;
                                self.read_buffer.rotate_left(size);
                                self.read_buffer_received -= size;

                                message_channel.send(
                                    ServerMessage::new(
                                        ConnId(self.token),
                                        ServerMessagePayload::Message(msg)
                                    )
                                )?;
                            }
                        }
                    }
                },
                Err(ref e) if would_block(e) => break,
                Err(e) => return Err(e)?,
            }
        }

        Ok(false)
    }

    fn try_do_writing(&mut self) -> Result<bool> {
        loop {
            match self.socket.write(&self.write_buffer[self.write_buffer_sent..]) {
                Ok(0) => return Ok(true),
                Ok(n) => {
                    self.write_buffer_sent += n;
                    if self.write_buffer_sent >= self.write_buffer.len() {
                        self.currently_writing_message = None;
                        self.try_setup_writing_next_message()?;
                    }
                },
                Err(ref e) if would_block(e) => break,
                Err(e) => return Err(e)?,
            }
        }

        Ok(false)
    }
}

pub fn start_server<TMessage: UserMessage>(addr: SocketAddr) -> Result<Server<TMessage>> {
    let (incoming_sender, incoming_receiver) = channel();
    let (outgoing_sender, outgoing_receiver) = channel();
    
    let mut token_generator = TokenGenerator::new(0);
    
    let poll = Poll::new()?;
    let waker_token = token_generator.next();
    let waker = Waker::new(poll.registry(), waker_token)?;

    let handle = thread::spawn(move || internal_start_server(
        addr, poll, token_generator, waker_token, incoming_sender, outgoing_receiver
    ));

    Ok(
        Server {
            incoming_messages: incoming_receiver,
            outgoing_messages: outgoing_sender,

            poll_waker: waker,

            thread_join_handle: handle,
        }
    )
}

fn internal_start_server<TMessage: UserMessage>(
    addr: SocketAddr,
    mut poll: Poll,
    mut token_generator: TokenGenerator,
    waker_token: Token,
    mut incoming_sender: Sender<ServerMessage<TMessage>>,
    outgoing_receiver: Receiver<(ConnId, TMessage)>,
) -> Result<()> {
    let mut all_sockets = HashMap::new();
    
    let mut events = Events::with_capacity(1024);

    let server_token = token_generator.next();
    let mut server = TcpListener::bind(addr)?;
    poll.registry().register(&mut server, server_token, Interest::READABLE)?;

    loop {
        poll.poll(&mut events, None)?;

        for event in &events {
            let token = event.token();
            let mut client_disconnected = false;
            
            if token == server_token {
                handle_client_connection(&server, &mut token_generator, &mut all_sockets, poll.registry(), &incoming_sender)?;
            } else if token == waker_token {
                handle_waker_messages(&outgoing_receiver, &mut all_sockets, &poll)?;
            } else if let Some(conn) = all_sockets.get_mut(&token) {
                if event.is_readable() {
                    client_disconnected = conn.try_do_reading(&mut incoming_sender)?;
                } else if event.is_writable() {
                    client_disconnected = conn.try_do_writing()?;
                    if conn.currently_writing_message.is_none() {
                        poll.registry().reregister(&mut conn.socket, token, Interest::READABLE)?;
                    }
                }

                if client_disconnected {
                    poll.registry().deregister(&mut conn.socket)?;

                    incoming_sender.send(
                        ServerMessage::new(
                            ConnId(token),
                            ServerMessagePayload::ClientDisconnected
                        )
                    )?;
                }
            }

            if client_disconnected {
                all_sockets.remove(&token);
            }
        }
    }
}

fn handle_waker_messages<TMessage: UserMessage>(
    outgoing_receiver: &Receiver<(ConnId, TMessage)>,
    all_sockets: &mut HashMap<Token, Connection<TMessage>>,
    poll: &Poll
) -> Result<()> {
    loop {
        match outgoing_receiver.try_recv() {
            Ok((ConnId(token), msg)) => {
                if let Some(conn) = all_sockets.get_mut(&token) {
                    conn.messages_to_write.push(msg);
                    conn.try_setup_writing_next_message()?;
                    poll.registry().reregister(
                        &mut conn.socket,
                        token,
                        Interest::READABLE | Interest::WRITABLE
                    )?;
                }
            },
            Err(std::sync::mpsc::TryRecvError::Empty) => break,
            Err(e) => return Err(e)?,
        }
    }

    Ok(())
}

fn handle_client_connection<TMessage: UserMessage>(
    server: &TcpListener,
    token_generator: &mut TokenGenerator,
    all_sockets: &mut HashMap<Token, Connection<TMessage>>,
    registry: &Registry,
    tx: &Sender<ServerMessage<TMessage>>
) -> Result<()> {
    loop {
        let mut socket = match server.accept() {
            Ok((s, _)) => s,
            Err(ref e) if would_block(&e) => break,
            Err(e) => return Err(e)?,
        };

        let token = token_generator.next();
        registry.register(&mut socket, token, Interest::READABLE)?;
        all_sockets.insert(token, Connection::new(socket, token));

        let msg = ServerMessage::new(
            ConnId(token),
            ServerMessagePayload::ClientConnected
        );
        tx.send(msg)?;
    }

    Ok(())
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

struct TokenGenerator {
    next: usize,
}

impl TokenGenerator {
    fn new(init: usize) -> Self {
        Self { next: init }
    }

    fn next(&mut self) -> Token {
        self.next += 1;
        Token(self.next - 1)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
