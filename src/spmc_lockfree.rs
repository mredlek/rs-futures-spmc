use ::std::cell::RefCell;
use ::std::fmt::{Debug, Formatter, Result as FmtResult};
use ::std::sync::Arc;
use ::std::sync::atomic::{AtomicBool, AtomicUsize, AtomicPtr, Ordering};
use ::std::ptr;

use ::futures::{Async,AsyncSink,Poll,StartSend, Sink,Stream};
use ::futures::task::{Task, current as current_task};

struct SharedData<T>
    where T: Clone
{
    slots: Vec<Slot<T>>,
    write_interest: AtomicUsize,
    closed: AtomicBool,
    sender_data_stash: RefCell<Option<Vec<Option<T>>>>
}

impl<T> SharedData<T>
        where T: Clone
{
    fn new(data: &mut Vec<Option<T>>) -> SharedData<T>
    {
        let dataptr = data.as_mut_ptr();
        let mut slots = Vec::with_capacity(data.capacity());
        for index in 0 .. data.capacity() {
            slots.push(Slot::new(unsafe { dataptr.offset(index as isize) }))
        }

        let none : Option<Vec<Option<T>>> = None;

        SharedData::<T> {
            slots: slots,
            write_interest: AtomicUsize::new(0usize),
            closed: AtomicBool::new(false),
            sender_data_stash: RefCell::new(none)
        }
    }

    fn successor(&self, index: usize) -> usize
    {
        (index + 1) % self.slots.capacity()
    }
}

impl<T> Debug for SharedData<T>
    where T: Clone
{
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        try!(fmt.debug_struct("SharedData")
            .field("write_interest", &self.write_interest.load(Ordering::Relaxed))
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish());
        for data in self.slots.iter().enumerate() {
            try!(fmt.write_fmt(format_args!("Slot {}: {:?}", data.0, data.1)))
        }
        Ok(())
    }
}

//The only not-sync field in SharedData is RefCell<Option<Vec<usize>>>, which is write-once read-never
//so not a problem.
unsafe impl<T> Sync for SharedData<T>
    where T: Clone
{
}

struct Slot<T>
    where T: Clone
{
    data: *mut Option<T>,
    read_interest: AtomicUsize,
    read_interest_locks: AtomicPtr<LocksNode>,
    write_interest_lock: AtomicPtr<Task>
}

impl<T> Slot<T>
    where T: Clone
{
    fn new(data: *mut Option<T>) -> Slot<T>
    {
        Slot {
            data: data,
            read_interest: AtomicUsize::new(0),
            read_interest_locks: AtomicPtr::new(ptr::null_mut()),
            write_interest_lock: AtomicPtr::new(ptr::null_mut())
        }
    }

    fn add_read_interest_lock(&self, task: Task)
    {
        let mut newlock = Box::into_raw(Box::new(LocksNode { next: ptr::null_mut(), task: task }));
        loop
        {
            let oldlock = self.read_interest_locks.load(Ordering::Relaxed);
            newlock = newlock.set_next(oldlock);
            let replacedlock = self.read_interest_locks.compare_and_swap(oldlock, newlock, Ordering::Relaxed);
            if ptr::eq(oldlock, replacedlock) {
                //The atomicptr was succesfully updated. We are finished
                break;
            }
            //Elsewise, we try again until we succeed
        }
    }

    fn add_write_interest_lock(&self, task: Task)
    {
        let task = Box::into_raw(Box::new(task));
        self.write_interest_lock.swap(task, Ordering::SeqCst);
    }

    fn cancel_write_interest_lock(&self)
    {
        self.write_interest_lock.swap(ptr::null_mut(), Ordering::SeqCst);
    }

    fn increase_read_interest(&self, order: Ordering)
    {
        self.read_interest.fetch_add(1usize, order);
    }

    fn decrease_read_interest(&self, order: Ordering)
    {
        if self.read_interest.fetch_sub(1usize, order) == 1usize {
            //There are no readers registered any more.
            //Unpark writelock
            let writelock = self.write_interest_lock.swap(ptr::null_mut(), Ordering::Relaxed);
            if !writelock.is_null() {
                unsafe { Box::from_raw(writelock) }.notify();
            }
        }
    }
}

impl<T> Debug for Slot<T>
    where T: Clone
{
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        fmt.debug_struct("Slot")
            .field("read_interest", &self.read_interest.load(Ordering::Relaxed))
            .field("read_interest_locks", &if self.read_interest_locks.load(Ordering::Relaxed).is_null() { "null".to_owned() } else { "not null".to_owned() })
            .field("write_interest_lock", &if self.write_interest_lock.load(Ordering::Relaxed).is_null() { "null".to_owned() } else { "not null".to_owned() })
            .finish()
    }
}

// Unsafe because it contains a pointer. The data is owned by Sender.
// It is once stashed in SharedData when the sender goes out of scope such that the pointer keeps valid
// SharedData will be destroyed when all receivers go out of scope because it is an Arc type
unsafe impl<T> Send for Slot<T>
    where T: Clone
{
}

struct LocksNode
{
    next: *mut LocksNode,
    task: Task
}

trait LocksNodeTrait
{
    fn notify(self);
    fn set_next(self, next: *mut LocksNode) -> Self;
}

impl LocksNodeTrait for *mut LocksNode
{
    fn notify(self)
    {
        let mut current = self;
        while !current.is_null() {
            let currentbox = unsafe { Box::from_raw(current) };
            currentbox.task.notify();
            current = currentbox.next;
        }
    }

    fn set_next(self, next: *mut LocksNode) -> Self
    {
        let mut unbox = unsafe { Box::from_raw(self) };
        unbox.next = next;
        Box::into_raw(unbox)
    }
}

pub struct Sender<T>
    where T: Clone
{
    shareddata: Arc<SharedData<T>>,
    data: Vec<Option<T>>
}

impl<T> Sender<T>
    where T: Clone
{
    pub fn add_receiver(&self) -> Receiver<T>
    {
        let mut current_write_interest = self.shareddata.write_interest.load(Ordering::SeqCst);
        loop {
            let write_interest = current_write_interest;
            self.shareddata.slots[write_interest].increase_read_interest(Ordering::SeqCst);
            current_write_interest = self.shareddata.write_interest.load(Ordering::SeqCst);
            if write_interest == current_write_interest {
                return Receiver {
                    shareddata: self.shareddata.clone(),
                    readinterest: write_interest
                };
            }
            //The write_interest was just updated. Tell it we are not interested anymore and try again
            self.shareddata.slots[write_interest].decrease_read_interest(Ordering::SeqCst);
        }
    }
}

impl<T> Debug for Sender<T>
    where T: Clone
{
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        fmt.debug_struct("Sender")
            .field("shareddata", &*self.shareddata)
            .finish()
    }
}

impl<T> Drop for Sender<T>
    where T: Clone
{
    fn drop(&mut self) {
        let mut data = Vec::new();
        ::std::mem::swap(&mut data, &mut self.data);
        (self as &mut Sink<SinkItem = T, SinkError = ()>).close().map(|_| ()).unwrap_or(());
        *self.shareddata.sender_data_stash.borrow_mut() = Some(data);
    }
}

impl<T> Sink for Sender<T>
    where T: Clone
{
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        fn start_send_exec<T>(this: &mut Sender<T>, item: T) -> ( Option<StartSend<T, ()>>, Option<T>, usize )
            where T: Clone
        {
            let write_interest = this.shareddata.write_interest.load(Ordering::Acquire);
            let write_interest_successor = this.shareddata.successor(write_interest);
            if this.shareddata.slots[write_interest_successor].read_interest.load(Ordering::Relaxed) == 0usize {
                //No readers available: we can write
                this.data[write_interest] = Some(item);
                this.shareddata.slots[write_interest_successor].cancel_write_interest_lock();
                this.shareddata.write_interest.store(write_interest_successor, Ordering::SeqCst);
                let unpark_read_interest_ptr = this.shareddata.slots[write_interest].read_interest_locks.swap(ptr::null_mut(), Ordering::SeqCst);
                if !unpark_read_interest_ptr.is_null() {
                    unpark_read_interest_ptr.notify();
                }
                (Some(Ok(AsyncSink::Ready)), None, write_interest_successor)
            } else {
                (None, Some(item), write_interest_successor)
            }
        }

        //Check if the streams is closed. In that case, just return Ready without saving it.
        //Returning an error here might also be an option.
        if self.shareddata.closed.load(Ordering::Acquire) {
            panic!("Start send called when the stream was closed!");
            //return Ok(AsyncSink::Ready)
        }
        let (start_send_exec_res, item, write_interest_successor) = start_send_exec(self, item);
        match start_send_exec_res {
            Some(res) => res,
            _ => {
                let item = item.unwrap(); //Guaranteed because we either return an item or and async result
                //let park = Box::into_raw(Box::new(::futures::task::park()));
                self.shareddata.slots[write_interest_successor].add_write_interest_lock(current_task());
                //self.shareddata.slots[write_interest_successor] .write_interest_lock.store(park, Ordering::SeqCst);

                //Try again
                let (start_send_exec_res, item, _) = start_send_exec(self, item);
                match start_send_exec_res {
                    Some(res) => res,
                    _ => { Ok(AsyncSink::NotReady(item.unwrap())) }
                }
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.shareddata.closed.store(true, Ordering::SeqCst);
        let write_interest = self.shareddata.write_interest.load(Ordering::SeqCst);
        let unpark_read_interest_ptr = self.shareddata.slots[write_interest].read_interest_locks.swap(ptr::null_mut(), Ordering::SeqCst);
        if !unpark_read_interest_ptr.is_null() {
            unpark_read_interest_ptr.notify();
        }
        Ok(Async::Ready(()))
    }
}

pub struct Receiver<T>
    where T: Clone
{
    shareddata: Arc<SharedData<T>>,
    readinterest: usize
}

impl<T> Debug for Receiver<T>
    where T: Clone
{
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        fmt.debug_struct("Receiver")
            .field("readinterest", &self.readinterest)
            .field("shareddata", &*self.shareddata)
            .finish()
    }
}

impl<T> Stream for Receiver<T>
    where T: Clone
{
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        fn read<T>(this: &mut Receiver<T>) -> Poll<Option<T>, ()>
            where T: Clone
        {
            let readinterest_successor = this.shareddata.successor(this.readinterest);
            this.shareddata.slots[readinterest_successor].increase_read_interest(Ordering::Relaxed);
            let item : &Option<T> = unsafe { &*this.shareddata.slots[this.readinterest].data };
            let item = item.clone().unwrap();
            this.shareddata.slots[this.readinterest].decrease_read_interest(Ordering::Release);
            this.readinterest = readinterest_successor;
            Ok(Async::Ready(Some(item)))
        }

        let write_interest = self.shareddata.write_interest.load(Ordering::Acquire);
        let closed = self.shareddata.closed.load(Ordering::SeqCst);
        if write_interest != self.readinterest {
            read(self)
        } else if closed {
            Ok(Async::Ready(None))
        } else {
            let task = current_task();
            self.shareddata.slots[self.readinterest].add_read_interest_lock(task);
            let current_write_interest = self.shareddata.write_interest.load(Ordering::SeqCst);
            let current_closed = self.shareddata.closed.load(Ordering::SeqCst);
            if write_interest != current_write_interest {
                //The writer dit increase its position. We can return that value at once.
                //We don't bother remove the interest_lock (unpark may be called multiple times)
                //Also, unpark doesn't need to be called now, because we return Ok
                read(self)
            } else if current_closed {
                Ok(Async::Ready(None))
            } else {
                Ok(Async::NotReady)
            }
        }
    }
}

impl<T> Clone for Receiver<T>
    where T: Clone
{
    fn clone(&self) -> Self {
        //We are interested in the same slot as self was.
        //Because self already registered this interest, we know it is at least 1.
        self.shareddata.slots[self.readinterest].increase_read_interest(Ordering::Release);

        Receiver {
            shareddata: self.shareddata.clone(),
            readinterest: self.readinterest.clone()
        }
    }
}

pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>)
    where T: Clone
{
    let mut data = Vec::with_capacity(buffer + 2);
    for _ in 0 .. buffer + 2 {
        data.push(None);
    }
    let shareddata = Arc::new(SharedData::new(&mut data));

    let sender = Sender { shareddata: shareddata.clone(), data: data };
    let receiver = sender.add_receiver();

    (sender, receiver)
}