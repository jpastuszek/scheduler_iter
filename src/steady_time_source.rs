use std::sync::{Mutex, Arc};
use std::thread::{self, Thread, sleep};
use std::time::Duration as StdDuration;
use time::{SteadyTime, Duration};

use time_source::*;

pub struct SteadyTimeSource {
    offset: SteadyTime,
    abort: Arc<Mutex<bool>>
}

impl SteadyTimeSource {
    pub fn new() -> SteadyTimeSource {
        SteadyTimeSource {
            offset: SteadyTime::now(),
            abort: Arc::new(Mutex::new(false))
        }
    }
}

impl TimeSource for SteadyTimeSource {
    fn now(&self) -> Duration {
        SteadyTime::now() - self.offset
    }
}

impl Wait for SteadyTimeSource {
    fn wait(&mut self, duration: Duration) {
        sleep(StdDuration::new(
            duration.num_seconds() as u64,
            (duration.num_nanoseconds().expect("sleep duration too large") - duration.num_seconds() * 1_000_000_000) as u32
        ));
    }
}

pub struct SteadyTimeSourceAbortHandle {
    waiter_thread: Thread,
    abort: Arc<Mutex<bool>>
}

impl Abort for SteadyTimeSourceAbortHandle {
    fn abort(&self) {
        {
            let mut abort = self.abort.lock().unwrap();
            *abort = true;
        }

        self.waiter_thread.unpark();
    }
}

impl AbortableWait for SteadyTimeSource {
    type AbortHandle = SteadyTimeSourceAbortHandle;

    fn abort_handle(&self) -> Self::AbortHandle {
        SteadyTimeSourceAbortHandle {
            waiter_thread: thread::current(),
            abort: self.abort.clone()
        }
    }

    fn abortable_wait(&mut self, duration: Duration) -> Result<(), WaitAbortedError> {
        //TODO: this can spuriously return
        thread::park_timeout(StdDuration::new(
            duration.num_seconds() as u64,
            (duration.num_nanoseconds().expect("sleep duration too large") - duration.num_seconds() * 1_000_000_000) as u32
        ));
        if *self.abort.lock().unwrap() {
            Err(WaitAbortedError)
        } else {
            Ok(())
        }
    }
}

impl FastForward for SteadyTimeSource {
    fn fast_forward(&mut self, duration: Duration) {
        assert!(duration > Duration::seconds(0), "fast_forward must be positive Duration");
        self.offset = self.offset - duration;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use time_source::*;
    use std::thread::spawn;
    use time::Duration;

    #[test]
    fn abortable_wait_early_abort() {
        let mut sts = SteadyTimeSource::new();

        let abort_handle = sts.abort_handle();
        spawn(move || {
            abort_handle.abort();
        });

        assert_eq!(sts.abortable_wait(Duration::seconds(2)), Err(WaitAbortedError));
    }

    #[test]
    fn abortable_wait_no_abort() {
        let mut sts = SteadyTimeSource::new();

        let _ = sts.abort_handle();

        assert_eq!(sts.abortable_wait(Duration::seconds(1)), Ok(()));
    }

    #[test]
    fn fast_forward() {
        let mut sts = SteadyTimeSource::new();

        let now = sts.now();
        assert!(now + Duration::seconds(1) > sts.now());

        sts.fast_forward(Duration::seconds(1));
        assert!(now + Duration::seconds(1) <= sts.now());
    }
}
