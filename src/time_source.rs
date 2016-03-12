use std::fmt;
use std::error::Error;
use time::Duration;

pub trait TimeSource {
    // Duration since this TimeSource was crated
    fn now(&self) -> Duration;
}

pub trait FastForward {
    fn fast_forward(&mut self, duration: Duration);
}

pub trait Wait {
    fn wait(&mut self, duration: Duration);
}

pub trait Abort: Send {
    fn abort(&self);
}

#[derive(Debug, PartialEq)]
pub struct WaitAbortedError;

impl Error for WaitAbortedError {
    fn description(&self) -> &str {
        "wait operation aborted from another thread"
    }
}

impl fmt::Display for WaitAbortedError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

pub trait AbortableWait {
    type AbortHandle: Abort;

    fn abort_handle(&self) -> Self::AbortHandle;
    fn abortable_wait(&mut self, duration: Duration) -> Result<(), WaitAbortedError>;
}

