extern crate rs_futures_spmc;
extern crate futures;

use ::std::sync::Arc;
use ::std::sync::atomic::{AtomicBool, Ordering};
use ::rs_futures_spmc::{channel, /*Sender,*/ Receiver};
use ::futures::{lazy, Async ,AsyncSink, Future, Stream, Sink, Poll};
use ::std::thread::{spawn as threadspawn, JoinHandle};

#[test]
fn test_one_receiver_no_buffer()
{
    let (mut tx, mut rx) = channel::<i32>(0);

    lazy(move || {
        assert_eq!(tx.start_send(0), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(1), Ok(AsyncSink::NotReady(1)));

        assert_eq!(rx.poll(), Ok(Async::Ready(Some(0))));
        assert_eq!(rx.poll(), Ok(Async::NotReady));

        assert_eq!(tx.start_send(2), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(3), Ok(AsyncSink::NotReady(3)));

        assert_eq!(rx.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(rx.poll(), Ok(Async::NotReady));

        assert_eq!(tx.start_send(3), Ok(AsyncSink::Ready));
        assert_eq!(rx.poll(), Ok(Async::Ready(Some(3))));

        Ok::<(), ()>(())
    }).wait().unwrap();
}

#[test]
fn test_three_receivers_no_buffer()
{
    let (mut tx, mut rx1) = channel::<i32>(0);

    lazy(move || {
        let mut rx2 = rx1.clone();

        assert_eq!(tx.start_send(0), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(1), Ok(AsyncSink::NotReady(1)));

        assert_eq!(rx1.poll(), Ok(Async::Ready(Some(0))));
        assert_eq!(rx1.poll(), Ok(Async::NotReady));

        let mut rx3 = rx1.clone();

        assert_eq!(rx2.poll(), Ok(Async::Ready(Some(0))));
        assert_eq!(rx2.poll(), Ok(Async::NotReady));
        assert_eq!(rx3.poll(), Ok(Async::NotReady));

        assert_eq!(tx.start_send(1), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(2), Ok(AsyncSink::NotReady(2)));

        assert_eq!(rx1.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(rx1.poll(), Ok(Async::NotReady));
        assert_eq!(rx2.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(rx2.poll(), Ok(Async::NotReady));
        assert_eq!(rx3.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(rx3.poll(), Ok(Async::NotReady));

        assert_eq!(tx.start_send(2), Ok(AsyncSink::Ready));
        assert_eq!(rx1.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(rx2.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(rx3.poll(), Ok(Async::Ready(Some(2))));

        Ok::<(), ()>(())
    }).wait().unwrap();
}

#[test]
fn test_one_receiver_with_buffer()
{
    let (mut tx, mut rx) = channel::<i32>(1);

    lazy(move || {
        assert_eq!(tx.start_send(0), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(1), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(2), Ok(AsyncSink::NotReady(2)));

        assert_eq!(rx.poll(), Ok(Async::Ready(Some(0))));
        assert_eq!(rx.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(rx.poll(), Ok(Async::NotReady));

        assert_eq!(tx.start_send(2), Ok(AsyncSink::Ready));

        assert_eq!(rx.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(rx.poll(), Ok(Async::NotReady));
        assert_eq!(tx.start_send(3), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(4), Ok(AsyncSink::Ready));
        assert_eq!(tx.start_send(5), Ok(AsyncSink::NotReady(5)));
        assert_eq!(rx.poll(), Ok(Async::Ready(Some(3))));
        assert_eq!(rx.poll(), Ok(Async::Ready(Some(4))));

        Ok::<(), ()>(())
    }).wait().unwrap();
}

struct CheckStream
{
    inner: Receiver<i32>,
    last_was_ready: Arc<AtomicBool>
}

impl Stream for CheckStream
{
    type Item = i32;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let pollresult = self.inner.poll();
        assert!(!pollresult.is_err());
        match pollresult {
            Ok(Async::Ready(Some(_))) => { self.last_was_ready.store(true, Ordering::SeqCst); },
            Ok(Async::Ready(None)) => { },
            Ok(Async::NotReady) => { /*assert!(self.lastWasReady.load(Ordering::Relaxed), "Polled when stream was not yet ready"); */ self.last_was_ready.store(false, Ordering::SeqCst); }
            Err(_) => { assert!(false, "Received error")}
        };
        pollresult
    }
}

fn run_receiver_in_thread(receiver: Receiver<i32>, last_was_ready: Arc<AtomicBool>) -> JoinHandle<Result<Vec<i32>,()>>
{
    threadspawn(move || {
        last_was_ready.store(true, Ordering::Relaxed);
        let checkstream = CheckStream { inner: receiver, last_was_ready: last_was_ready };
        checkstream.collect().wait()
    })
}

#[test]
fn test_receiver_different_thread()
{
    let (sender, receiver) = channel::<i32>(0);
    let receiver_not_reader = Arc::new(AtomicBool::new(false));

    let receiver_handle = run_receiver_in_thread(receiver, receiver_not_reader.clone());

    ::futures::stream::iter(vec![Ok(0) as Result<i32,()>, Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)].into_iter()).forward(sender)
        .wait()
        .expect("Error forwarding stream to sender");

    receiver_handle.join().expect("Joining threads failed").unwrap();
}

#[test]
fn test_receiver_different_thread_slow_sender()
{
    let (mut sender, receiver) = channel::<i32>(0);
    let receiver_ready = Arc::new(AtomicBool::new(true));

    let receiver_handle = run_receiver_in_thread(receiver, receiver_ready.clone());

    assert_eq!(sender.start_send(0), Ok(AsyncSink::Ready));
    //Wait for the receiver to be not reaady

    while receiver_ready.load(Ordering::SeqCst) {}

    assert_eq!(sender.close(), Ok(Async::Ready(())));
    /*::futures::stream::iter(vec![Ok(1) as Result<i32, ()>, Ok(2), Ok(3)].into_iter()).forward(sender)
        .wait()
        .expect("Error forwaring stream to sender");*/

    receiver_handle.join().expect("Joining threads failed").unwrap();
}
