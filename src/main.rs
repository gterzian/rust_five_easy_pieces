#[macro_use]
extern crate crossbeam_channel;
extern crate rayon;

use crossbeam_channel::unbounded;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

fn main() {}

#[test]
fn first() {
    /// The messages sent from the "main" component,
    /// to the other component running in parallel.
    enum WorkMsg {
        Work(u8),
    }

    /// The messages sent from the "parallel" component,
    /// back to the "main component".
    enum ResultMsg {
        Result(u8),
    }

    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();

    // Spawn another component in parallel.
    let worker = thread::spawn(move || {
        // Receive, and handle, messages,
        // until told to exit.
        for WorkMsg::Work(num) in work_receiver {
            // Perform some "work", sending back the result.
            result_sender.send(ResultMsg::Result(num)).unwrap();
        }
    });

    // Send two pieces of "work",
    // followed by a request to exit.
    work_sender.send(WorkMsg::Work(0)).unwrap();
    work_sender.send(WorkMsg::Work(1)).unwrap();
    drop(work_sender);

    // A counter of work performed.
    let mut counter = 0;

    for ResultMsg::Result(num) in result_receiver {
        // Assert that we're receiving results
        // in the same order that the requests were sent.
        assert_eq!(num, counter);
        counter += 1;
    }
    // Assert that we're exiting
    // after having received two work results.
    assert_eq!(2, counter);
    // Manual join is also an anti-pattern, better to use something like jod_tread.
    worker.join().unwrap()
}

#[test]
fn second() {
    enum WorkMsg {
        Work(u8),
        Exit,
    }

    enum ResultMsg {
        Result(u8),
        Exited,
    }

    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let _ = thread::spawn(move || loop {
        match work_receiver.recv() {
            Ok(WorkMsg::Work(num)) => {
                // Clone the result sender, and move the clone
                // into the spawned worker.
                let result_sender = result_sender.clone();
                pool.spawn(move || {
                    // From a worker thread,
                    // do some "work",
                    // and send the result back.
                    let _ = result_sender.send(ResultMsg::Result(num));
                });
            }
            Ok(WorkMsg::Exit) => {
                let _ = result_sender.send(ResultMsg::Exited);
                break;
            }
            _ => panic!("Error receiving a WorkMsg."),
        }
    });

    let _ = work_sender.send(WorkMsg::Work(0));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Exit);

    loop {
        match result_receiver.recv() {
            Ok(ResultMsg::Result(_)) => {
                // We cannot make assertions about ordering anymore.
            }
            Ok(ResultMsg::Exited) => {
                // And neither can we make assertions
                // that the results have been received
                // prior to the exited message.
                break;
            }
            _ => panic!("Error receiving a ResultMsg."),
        }
    }
}

#[test]
fn third() {
    enum WorkMsg {
        Work(u8),
        Exit,
    }

    enum ResultMsg {
        Result(u8),
        Exited,
    }

    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();
    // Add a new channel, used by workers
    // to notity the "parallel" component of having completed a unit of work.
    let (pool_result_sender, pool_result_receiver) = unbounded();
    let mut ongoing_work = 0;
    let mut exiting = false;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let _ = thread::spawn(move || loop {
        select! {
            recv(work_receiver) -> msg => {
                match msg {
                    Ok(WorkMsg::Work(num)) => {
                        let result_sender = result_sender.clone();
                        let pool_result_sender = pool_result_sender.clone();

                        // Note that we're starting a new unit of work on the pool.
                        ongoing_work += 1;

                        pool.spawn(move || {
                            // 1. Send the result to the main component.
                            let _ = result_sender.send(ResultMsg::Result(num));

                            // 2. Let the parallel component know we've completed a unit of work.
                            let _ = pool_result_sender.send(());
                        });
                    },
                    Ok(WorkMsg::Exit) => {
                        // Note that we've received the request to exit.
                        exiting = true;

                        // If there is no ongoing work,
                        // we can immediately exit.
                        if ongoing_work == 0 {
                            let _ = result_sender.send(ResultMsg::Exited);
                            break;
                        }
                    },
                    _ => panic!("Error receiving a WorkMsg."),
                }
            },
            recv(pool_result_receiver) -> _ => {
                if ongoing_work == 0 {
                    panic!("Received an unexpected pool result.");
                }

                // Note that a unit of work has been completed.
                ongoing_work -=1;

                // If there is no more ongoing work,
                // and we've received the request to exit,
                // now is the time to exit.
                if ongoing_work == 0 && exiting {
                    let _ = result_sender.send(ResultMsg::Exited);
                    break;
                }
            },
        }
    });

    let _ = work_sender.send(WorkMsg::Work(0));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Exit);

    let mut counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ResultMsg::Result(_)) => {
                // Count the units of work that have been completed.
                counter += 1;
            }
            Ok(ResultMsg::Exited) => {
                // Assert that we're exiting after having received
                // all results.
                assert_eq!(2, counter);
                break;
            }
            _ => panic!("Error receiving a ResultMsg."),
        }
    }
}

#[test]
fn fourth() {
    enum WorkMsg {
        Work(u8),
        Exit,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum WorkPerformed {
        FromCache,
        New,
    }

    enum ResultMsg {
        Result(u8, WorkPerformed),
        Exited,
    }

    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();
    let (pool_result_sender, pool_result_receiver) = unbounded();
    let mut ongoing_work = 0;
    let mut exiting = false;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    // A cache of "work", shared by the workers on the pool.
    let cache: Arc<Mutex<HashMap<u8, u8>>> = Arc::new(Mutex::new(HashMap::new()));

    let _ = thread::spawn(move || loop {
        select! {
            recv(work_receiver) -> msg => {
                match msg {
                    Ok(WorkMsg::Work(num)) => {
                        let result_sender = result_sender.clone();
                        let pool_result_sender = pool_result_sender.clone();
                        let cache = cache.clone();
                        ongoing_work += 1;

                        pool.spawn(move || {
                            {
                                // Start of critical section on the cache.
                                let cache = cache.lock().unwrap();
                                if let Some(result) = cache.get(&num) {
                                    // We're getting a result from the cache,
                                    // send it back,
                                    // along with a flag indicating we got it from the cache.
                                    let _ = result_sender.send(ResultMsg::Result(result.clone(), WorkPerformed::FromCache));
                                    let _ = pool_result_sender.send(());
                                    return;
                                }
                                // End of critical section on the cache.
                            }

                            // Perform "expensive work" outside of the critical section.
                            // work work work work work work...

                            // Send the result back, indicating we had to perform the work.
                            let _ = result_sender.send(ResultMsg::Result(num.clone(), WorkPerformed::New));

                            // Store the result of the "expensive work" in the cache.
                            let mut cache = cache.lock().unwrap();
                            cache.insert(num.clone(), num);

                            let _ = pool_result_sender.send(());
                        });
                    },
                    Ok(WorkMsg::Exit) => {
                        exiting = true;
                        if ongoing_work == 0 {
                            let _ = result_sender.send(ResultMsg::Exited);
                            break;
                        }
                    },
                    _ => panic!("Error receiving a WorkMsg."),
                }
            },
            recv(pool_result_receiver) -> _ => {
                if ongoing_work == 0 {
                    panic!("Received an unexpected pool result.");
                }
                ongoing_work -=1;
                if ongoing_work == 0 && exiting {
                    let _ = result_sender.send(ResultMsg::Exited);
                    break;
                }
            },
        }
    });

    let _ = work_sender.send(WorkMsg::Work(0));
    // Send two requests for the same "work"
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Exit);

    let mut counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ResultMsg::Result(_num, _cached)) => {
                counter += 1;
                // We cannot make assertions about `cached`.
            }
            Ok(ResultMsg::Exited) => {
                assert_eq!(3, counter);
                break;
            }
            _ => panic!("Error receiving a ResultMsg."),
        }
    }
}

#[test]
fn fifth() {
    enum WorkMsg {
        Work(u8),
        Exit,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum WorkPerformed {
        FromCache,
        New,
    }

    enum CacheState {
        Ready,
        WorkInProgress,
    }

    enum ResultMsg {
        Result(u8, WorkPerformed),
        Exited,
    }

    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();
    let (pool_result_sender, pool_result_receiver) = unbounded();
    let mut ongoing_work = 0;
    let mut exiting = false;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let cache: Arc<Mutex<HashMap<u8, u8>>> = Arc::new(Mutex::new(HashMap::new()));

    // A new `cache_state` shared piece of data, indicating whether for a given number,
    // the cache is ready to be read from.
    let cache_state: Arc<Mutex<HashMap<u8, Arc<(Mutex<CacheState>, Condvar)>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let _ = thread::spawn(move || loop {
        select! {
            recv(work_receiver) -> msg => {
                match msg {
                    Ok(WorkMsg::Work(num)) => {
                        let result_sender = result_sender.clone();
                        let pool_result_sender = pool_result_sender.clone();
                        let cache = cache.clone();
                        let cache_state = cache_state.clone();
                        ongoing_work += 1;
                        pool.spawn(move || {
                            {

                                let (lock, cvar) = {
                                    // Start of critical section on `cache_state`.
                                    let mut state_map = cache_state.lock().unwrap();
                                    &*state_map
                                        .entry(num.clone())
                                        .or_insert_with(|| {
                                            Arc::new((
                                                Mutex::new(CacheState::Ready),
                                                Condvar::new(),
                                            ))
                                        })
                                        .clone()
                                    // End of critical section on `cache_state`.
                                };

                                // Start of critical section on `state`.
                                let mut state = lock.lock().unwrap();

                                // Note: the `while` loop is necessary
                                // for thd logic to be robust to spurious wake-ups.
                                while let CacheState::WorkInProgress = *state {
                                    // Block until the state is `CacheState::Ready`.
                                    //
                                    // Note: this will atomically release the lock,
                                    // and reacquire it on wake-up.
                                    let current_state = cvar
                                        .wait(state)
                                        .unwrap();
                                    state = current_state;
                                }

                                // Here, since we're out of the loop,
                                // we can be certain that
                                // `state == CacheState::Ready`

                                let cache = cache.lock().unwrap();
                                if let Some(result) = cache.get(&num) {
                                    // If we find a result in the cache,
                                    // leave `state` as ready,
                                    // send the result back,
                                    // and return.
                                    let _ = result_sender.send(ResultMsg::Result(result.clone(), WorkPerformed::FromCache));
                                    let _ = pool_result_sender.send(());

                                    // Also make sure to signal the next thread in line,
                                    // if any, that the cache is ready to be read from.
                                    cvar.notify_one();
                                    return;
                                }

                                // If we didn't find a result in the cache,
                                // switch the state to in-progress.
                                *state = CacheState::WorkInProgress;

                                // End of critical section on `state`.
                            }

                            // Do some "expensive work", outside of any critical section.

                            let _ = result_sender.send(ResultMsg::Result(num.clone(), WorkPerformed::New));

                            // Insert the result of the work into the cache.
                            let mut cache = cache.lock().unwrap();
                            cache.insert(num.clone(), num);

                            // Re-enter the critical section on `cache_state`.
                            let (lock, cvar) = {
                                let mut state_map = cache_state.lock().unwrap();
                                &*state_map
                                    .get_mut(&num)
                                    .expect("Entry in cache state to have been previously inserted")
                                    .clone()
                            };
                            let mut state = lock.lock().unwrap();

                            // Switch the state to ready.
                            *state = CacheState::Ready;

                            // Notify the waiting thread, if any, that the state has changed.
                            // This can be done while still inside the critical section.
                            cvar.notify_one();

                            let _ = pool_result_sender.send(());
                        });
                    },
                    Ok(WorkMsg::Exit) => {
                        exiting = true;
                        if ongoing_work == 0 {
                            let _ = result_sender.send(ResultMsg::Exited);
                            break;
                        }
                    },
                    _ => panic!("Error receiving a WorkMsg."),
                }
            },
            recv(pool_result_receiver) -> _ => {
                if ongoing_work == 0 {
                    panic!("Received an unexpected pool result.");
                }
                ongoing_work -=1;
                if ongoing_work == 0 && exiting {
                    let _ = result_sender.send(ResultMsg::Exited);
                    break;
                }
            },
        }
    });

    let _ = work_sender.send(WorkMsg::Work(0));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Exit);

    let mut counter = 0;

    // A new counter for work on 1.
    let mut work_one_counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ResultMsg::Result(num, cached)) => {
                counter += 1;

                if num == 1 {
                    work_one_counter += 1;
                }

                // Now we can assert that by the time
                // the second result for 1 has been received,
                // it came from the cache.
                if num == 1 && work_one_counter == 2 {
                    assert_eq!(cached, WorkPerformed::FromCache);
                }
            }
            Ok(ResultMsg::Exited) => {
                assert_eq!(3, counter);
                break;
            }
            _ => panic!("Error receiving a ResultMsg."),
        }
    }
}
