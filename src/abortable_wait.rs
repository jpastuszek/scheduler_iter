use std::fmt;
use std::error::Error;
use std::any::Any;
use time::Duration;

use super::{TimeSource, Wait, AbortableWait, WaitAbortedError};
use super::{Scheduler, Schedule};

pub enum AbortableWaitTimeoutError<Token> {
    Empty,
    Timeout,
    Overrun(Vec<Token>),
    Aborted
}

impl<Token> PartialEq for AbortableWaitTimeoutError<Token> where Token: PartialEq<Token> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&AbortableWaitTimeoutError::Empty, &AbortableWaitTimeoutError::Empty) => true,
            (&AbortableWaitTimeoutError::Timeout, &AbortableWaitTimeoutError::Timeout) => true,
            (&AbortableWaitTimeoutError::Overrun(ref tokens), &AbortableWaitTimeoutError::Overrun(ref other_tokens)) => tokens == other_tokens,
            (&AbortableWaitTimeoutError::Aborted, &AbortableWaitTimeoutError::Aborted) => true,
            _ => false
        }
    }
}

impl<Token> fmt::Debug for AbortableWaitTimeoutError<Token> where Token: fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &AbortableWaitTimeoutError::Empty => write!(f, "AbortableWaitTimeoutError::Empty"),
            &AbortableWaitTimeoutError::Timeout => write!(f, "AbortableWaitTimeoutError::Timeout"),
            &AbortableWaitTimeoutError::Overrun(ref tokens) => write!(f, "AbortableWaitTimeoutError::Overrun({:?})", tokens),
            &AbortableWaitTimeoutError::Aborted => write!(f, "AbortableWaitTimeoutError::Aborted")
        }
    }
}

impl<Token> fmt::Display for AbortableWaitTimeoutError<Token> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &AbortableWaitTimeoutError::Empty => write!(f, "scheduler is empty"),
            &AbortableWaitTimeoutError::Timeout => write!(f, "timedout while waiting for tokens"),
            &AbortableWaitTimeoutError::Overrun(ref tokens) => write!(f, "scheduler overrun {} tokens", tokens.len()),
            &AbortableWaitTimeoutError::Aborted => write!(f, "wait operation was aborted from another thread")
        }
    }
}

impl<Token> Error for AbortableWaitTimeoutError<Token> where Token: fmt::Debug + Any {
    fn description(&self) -> &str {
        "problem while waiting for next schedule with timeout"
    }
}
impl<Token, TS> Scheduler<Token, TS> where TS: TimeSource + Wait + AbortableWait, Token: Clone {
    pub fn abort_handle(&self) -> <TS as AbortableWait>::AbortHandle {
        self.time_source.abort_handle()
    }

    pub fn abortable_wait_timeout(&mut self, timeout: Duration) -> Result<Vec<Token>, AbortableWaitTimeoutError<Token>> {
        match self.next() {
            Some(schedule) => match schedule {
                Schedule::NextIn(duration) => {
                    if duration > timeout {
                        if let Err(WaitAbortedError) = self.time_source.abortable_wait(timeout) {
                            return Err(AbortableWaitTimeoutError::Aborted);
                        };
                        return Err(AbortableWaitTimeoutError::Timeout);
                    }
                    self.time_source.wait(duration);
                    self.abortable_wait_timeout(Duration::zero())
                },
                Schedule::Overrun(overrun_tokens) => {
                    Err(AbortableWaitTimeoutError::Overrun(overrun_tokens))
                },
                Schedule::Current(tokens) => {
                    Ok(tokens)
                }
            },
            None => Err(AbortableWaitTimeoutError::Empty)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::{Abort, AbortableWait};
    use super::super::Scheduler;
    use time::Duration;
    use std::thread::spawn;

    #[test]
    fn abortable_wait_timeout_real_time() {
        let mut scheduler = Scheduler::new(Duration::milliseconds(100));

        scheduler.after(Duration::milliseconds(100), 0);
        scheduler.after(Duration::seconds(20), 1);
        scheduler.after(Duration::seconds(40), 2);

        let abort_handle = scheduler.abort_handle();

        assert_eq!(scheduler.abortable_wait_timeout(Duration::seconds(2)), Ok(vec![0]));
        assert_eq!(scheduler.abortable_wait_timeout(Duration::milliseconds(20)), Err(AbortableWaitTimeoutError::Timeout));

        spawn(move || {
            abort_handle.abort();
        });

        assert_eq!(scheduler.abortable_wait_timeout(Duration::seconds(2)), Err(AbortableWaitTimeoutError::Aborted));
    }
}
