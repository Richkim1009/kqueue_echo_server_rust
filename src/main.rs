use futures::{
    future::{BoxFuture, FutureExt},
    task::{waker_ref, ArcWake},
};
use nix::{
    errno::Errno,
    sys::{
        event::{EventFilter, ev_set, EventFlag, kevent, kqueue, KEvent}
    },
    unistd::write,
};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    io::{BufRead, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::io::{AsRawFd, RawFd},
    pin::Pin,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker}
};

fn write_eventfd(fd: RawFd, n: usize) {
    let ptr = &n as *const usize as *const u8;
    let val = unsafe {
        std::slice::from_raw_parts(ptr, std::mem::size_of_val(&n))
    };
    write(fd, &val).unwrap();
}

fn main() {
    println!("Hello, world!");
}
