use time::Duration;

use time_source::*;

pub struct MockTimeSource {
    current_time: Duration
}

impl MockTimeSource {
    pub fn new() -> MockTimeSource {
        MockTimeSource {
            current_time: Duration::seconds(0)
        }
    }
}

impl FastForward for MockTimeSource {
    fn fast_forward(&mut self, duration: Duration) {
        self.current_time = self.current_time + duration;
    }
}

impl TimeSource for MockTimeSource {
    fn now(&self) -> Duration {
        self.current_time
    }
}

pub struct MockTimeSourceWait {
    current_time: Duration
}

impl MockTimeSourceWait {
    pub fn new() -> MockTimeSourceWait {
        MockTimeSourceWait {
            current_time: Duration::seconds(0)
        }
    }
}

impl FastForward for MockTimeSourceWait {
    fn fast_forward(&mut self, duration: Duration) {
        self.current_time = self.current_time + duration;
    }
}

impl Wait for MockTimeSourceWait {
    fn wait(&mut self, duration: Duration) {
        self.current_time = self.current_time + duration;
    }
}

impl TimeSource for MockTimeSourceWait {
    fn now(&self) -> Duration {
        self.current_time
    }
}

// Used to test minimum requred traits on token type
#[derive(Clone)]
#[allow(dead_code)]
pub enum OpaqueToken {
    Zero,
    One,
    Two,
}

//TODO: does testing this makes sense?
impl OpaqueToken {
    pub fn expect(&self, t: OpaqueToken) {
        match (self, t) {
            (&Zero, Zero) => (),
            (&One, One) => (),
            (&Two, Two) => (),
            (_, _) => panic!("OpaqueToken mismatch")
        }
    }
}

pub use self::OpaqueToken::{Zero, One, Two};
