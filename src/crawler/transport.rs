use nix::sys::socket::{self, AddressFamily, SockFlag, SockType, SockaddrIn};
use std::io;
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::os::fd::{AsRawFd, FromRawFd};
use tokio::net::UdpSocket;
use tracing::{debug, warn};

use super::codec::Message;

const MAX_UDP_PAYLOAD: usize = 65507;

pub struct Transport {
    socket: UdpSocket,
    buffer: Vec<u8>,
    on_message: Box<dyn Fn(Message, SocketAddr) + Send + Sync>,
    on_congestion: Option<Box<dyn Fn() + Send + Sync>>,
}

impl Transport {
    /// Create a new transport bound to the given address
    pub async fn new(
        laddr: SocketAddr,
        on_message: Box<dyn Fn(Message, SocketAddr) + Send + Sync>,
        on_congestion: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> io::Result<Self> {
        // Create raw socket using nix
        let socket_fd = socket::socket(
            AddressFamily::Inet,
            SockType::Datagram,
            SockFlag::empty(),
            None,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Bind to address
        let sockaddr = match laddr {
            SocketAddr::V4(addr) => SockaddrIn::from(addr),
            SocketAddr::V6(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "IPv6 not supported yet",
                ))
            }
        };
        socket::bind(socket_fd.as_raw_fd(), &sockaddr)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Convert to std UdpSocket then to tokio UdpSocket
        let std_socket = unsafe { StdUdpSocket::from_raw_fd(socket_fd.as_raw_fd()) };
        std_socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_socket)?;

        Ok(Self {
            socket,
            buffer: vec![0u8; MAX_UDP_PAYLOAD],
            on_message,
            on_congestion,
        })
    }

    /// Start receiving messages
    pub async fn run(mut self) {
        loop {
            match self.socket.recv_from(&mut self.buffer).await {
                Ok((n, peer_addr)) => {
                    if n == 0 {
                        // Zero-length datagram is valid but we ignore it
                        continue;
                    }

                    // Try to parse as bencode message
                    match serde_bencode::from_bytes::<Message>(&self.buffer[..n]) {
                        Ok(msg) => {
                            (self.on_message)(msg, peer_addr);
                        }
                        Err(e) => {
                            debug!("Failed to parse message from {}: {}", peer_addr, e);
                        }
                    }
                }
                Err(e) => {
                    // Check for congestion errors
                    if let Some(errno) = e.raw_os_error() {
                        // EPERM (1) or ENOBUFS (105)
                        if errno == 1 || errno == 105 {
                            warn!("Network congestion detected (errno {})", errno);
                            if let Some(ref cb) = self.on_congestion {
                                cb();
                            }
                            continue;
                        }
                    }

                    // Other errors might mean socket closed
                    debug!("recv_from error: {}", e);
                    break;
                }
            }
        }
    }

    /// Send a message to a peer
    pub async fn send_message(&self, msg: &Message, peer_addr: SocketAddr) -> io::Result<()> {
        let data = serde_bencode::to_bytes(msg)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        match self.socket.send_to(&data, peer_addr).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Check for congestion errors
                if let Some(errno) = e.raw_os_error() {
                    if errno == 1 || errno == 105 {
                        warn!("Write congestion detected (errno {})", errno);
                        if let Some(ref cb) = self.on_congestion {
                            cb();
                        }
                        return Ok(()); // Don't propagate congestion as error
                    }
                }
                Err(e)
            }
        }
    }

    /// Get the local address
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transport_creation() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let transport = Transport::new(
            addr,
            Box::new(|_, _| {}),
            None,
        )
        .await
        .unwrap();

        assert!(transport.local_addr().is_ok());
    }
}
