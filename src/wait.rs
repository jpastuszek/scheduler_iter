use std::fmt;
use std::error::Error;
use std::any::Any;
use time::Duration;

use super::{TimeSource, Wait};
use super::{Scheduler, Schedule};

pub enum WaitError<Token> {
    Empty,
    Overrun(Vec<Token>)
}

impl<Token> PartialEq for WaitError<Token> where Token: PartialEq<Token> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&WaitError::Empty, &WaitError::Empty) => true,
            (&WaitError::Overrun(ref tokens), &WaitError::Overrun(ref other_tokens)) => tokens == other_tokens,
            _ => false
        }
    }
}

impl<Token> fmt::Debug for WaitError<Token> where Token: fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &WaitError::Empty => write!(f, "WaitError::Empty"),
            &WaitError::Overrun(ref tokens) => write!(f, "WaitError::Overrun({:?})", tokens)
        }
    }
}

impl<Token> fmt::Display for WaitError<Token> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &WaitError::Empty => write!(f, "scheduler is empty"),
            &WaitError::Overrun(ref tokens) => write!(f, "scheduler overrun {} tokens", tokens.len())
        }
    }
}

impl<Token> Error for WaitError<Token> where Token: fmt::Debug + Any {
    fn description(&self) -> &str {
        "problem while waiting for next schedule"
    }
}

pub enum WaitTimeoutError<Token> {
    Empty,
    Timeout,
    Overrun(Vec<Token>)
}

impl<Token> PartialEq for WaitTimeoutError<Token> where Token: PartialEq<Token> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&WaitTimeoutError::Empty, &WaitTimeoutError::Empty) => true,
            (&WaitTimeoutError::Timeout, &WaitTimeoutError::Timeout) => true,
            (&WaitTimeoutError::Overrun(ref tokens), &WaitTimeoutError::Overrun(ref other_tokens)) => tokens == other_tokens,
            _ => false
        }
    }
}

impl<Token> fmt::Debug for WaitTimeoutError<Token> where Token: fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &WaitTimeoutError::Empty => write!(f, "WaitError::Empty"),
            &WaitTimeoutError::Timeout => write!(f, "WaitError::Timeout"),
            &WaitTimeoutError::Overrun(ref tokens) => write!(f, "WaitError::Overrun({:?})", tokens)
        }
    }
}

impl<Token> fmt::Display for WaitTimeoutError<Token> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &WaitTimeoutError::Empty => write!(f, "scheduler is empty"),
            &WaitTimeoutError::Timeout => write!(f, "timedout while waiting for tokens"),
            &WaitTimeoutError::Overrun(ref tokens) => write!(f, "scheduler overrun {} tokens", tokens.len())
        }
    }
}

impl<Token> Error for WaitTimeoutError<Token> where Token: fmt::Debug + Any {
    fn description(&self) -> &str {
        "problem while waiting for next schedule with timeout"
    }
}

impl<Token, TS> Scheduler<Token, TS> where TS: TimeSource, Token: Clone {
    pub fn wait(&mut self) -> Result<Vec<Token>, WaitError<Token>> where TS: Wait {
        match self.next() {
            Some(schedule) => match schedule {
                Schedule::NextIn(duration) => {
                    self.time_source.wait(duration);
                    self.wait()
                },
                Schedule::Overrun(overrun_tokens) => {
                    Err(WaitError::Overrun(overrun_tokens))
                },
                Schedule::Current(tokens) => {
                    Ok(tokens)
                }
            },
            None => Err(WaitError::Empty)
        }
    }

    pub fn wait_timeout(&mut self, timeout: Duration) -> Result<Vec<Token>, WaitTimeoutError<Token>> where TS: Wait {
        match self.next() {
            Some(schedule) => match schedule {
                Schedule::NextIn(duration) => {
                    if duration > timeout {
                        self.time_source.wait(timeout);
                        return Err(WaitTimeoutError::Timeout);
                    }
                    self.time_source.wait(duration);
                    self.wait_timeout(Duration::zero())
                },
                Schedule::Overrun(overrun_tokens) => {
                    Err(WaitTimeoutError::Overrun(overrun_tokens))
                },
                Schedule::Current(tokens) => {
                    Ok(tokens)
                }
            },
            None => Err(WaitTimeoutError::Empty)
        }
    }

    pub fn try(&mut self) -> Option<Result<Vec<Token>, WaitError<Token>>> {
        match self.next() {
            Some(schedule) => match schedule {
                Schedule::NextIn(_) => {
                    None
                },
                Schedule::Overrun(overrun_tokens) => {
                    Some(Err(WaitError::Overrun(overrun_tokens)))
                },
                Schedule::Current(tokens) => {
                    Some(Ok(tokens))
                }
            },
            None => Some(Err(WaitError::Empty))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::{FastForward, Scheduler};
    use time::Duration;
    use mock_time_source::test::*;

    #[test]
    fn scheduler_wait() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSourceWait::new());

        scheduler.after(Duration::seconds(0), 0);
        scheduler.after(Duration::seconds(1), 1);
        scheduler.after(Duration::seconds(2), 2);

        assert_eq!(scheduler.wait(), Ok(vec![0]));
        assert_eq!(scheduler.wait(), Ok(vec![1]));
        assert_eq!(scheduler.wait(), Ok(vec![2]));
    }

    #[test]
    fn scheduler_wait_opeque_token() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSourceWait::new());

        scheduler.after(Duration::seconds(0), Zero);
        scheduler.after(Duration::seconds(1), One);
        scheduler.after(Duration::seconds(2), Two);

        match scheduler.wait() {
            Ok(tokens) => {
                assert_eq!(tokens.len(), 1);
                tokens.first().unwrap().expect(Zero)
            }
            _ => panic!("expected Ok")
        }
    }

    #[test]
    fn scheduler_wait_with_overrun() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSourceWait::new());

        scheduler.after(Duration::seconds(0), 0);
        scheduler.after(Duration::seconds(1), 1);
        scheduler.after(Duration::seconds(2), 2);

        scheduler.fast_forward(Duration::seconds(2));
        assert_eq!(scheduler.wait(), Err(WaitError::Overrun(vec![0, 1])));
        assert_eq!(scheduler.wait(), Ok(vec![2]));
    }

    #[test]
    fn scheduler_wait_timeout() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSourceWait::new());

        scheduler.after(Duration::seconds(0), 0);
        scheduler.after(Duration::seconds(1), 1);
        scheduler.after(Duration::seconds(2), 2);

        assert_eq!(scheduler.wait_timeout(Duration::seconds(2)), Ok(vec![0]));
        assert_eq!(scheduler.wait_timeout(Duration::milliseconds(500)), Err(WaitTimeoutError::Timeout));
    }

    #[test]
    fn scheduler_wait_timeout_opeque_token() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSourceWait::new());

        scheduler.after(Duration::seconds(0), Zero);
        scheduler.after(Duration::seconds(1), One);
        scheduler.after(Duration::seconds(2), Two);

        match scheduler.wait_timeout(Duration::seconds(2)) {
            Ok(tokens) => {
                assert_eq!(tokens.len(), 1);
                tokens.first().unwrap().expect(Zero)
            }
            _ => panic!("expected Ok")
        }
        match scheduler.wait_timeout(Duration::milliseconds(500)) {
            Err(WaitTimeoutError::Timeout) => (),
            _ => panic!("expected Err(WaitTimeoutError::Timeout)")
        }
    }

    #[test]
    fn scheduler_wait_real_time() {
        let mut scheduler = Scheduler::new(Duration::milliseconds(100));

        scheduler.after(Duration::milliseconds(0), 0);
        scheduler.after(Duration::milliseconds(100), 1);
        scheduler.after(Duration::milliseconds(200), 2);

        assert_eq!(scheduler.wait(), Ok(vec![0]));
        assert_eq!(scheduler.wait(), Ok(vec![1]));
        assert_eq!(scheduler.wait(), Ok(vec![2]));
    }

    #[test]
    fn scheduler_try() {
        let mut scheduler = Scheduler::with_time_source(Duration::nanoseconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(0), 0);
        scheduler.after(Duration::seconds(1), 1);
        scheduler.after(Duration::seconds(2), 2);

        assert_eq!(scheduler.try(), Some(Ok(vec![0])));
        assert_eq!(scheduler.try(), None);
        assert_eq!(scheduler.try(), None);

        scheduler.fast_forward(Duration::seconds(1));
        assert_eq!(scheduler.try(), Some(Ok(vec![1])));
        assert_eq!(scheduler.try(), None);

        scheduler.fast_forward(Duration::seconds(1));
        assert_eq!(scheduler.try(), Some(Ok(vec![2])));
        assert_eq!(scheduler.try(), Some(Err(WaitError::Empty)));
    }

    #[test]
    fn scheduler_try_with_overrun() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(0), 0);
        scheduler.after(Duration::seconds(1), 1);
        scheduler.after(Duration::seconds(2), 2);

        scheduler.fast_forward(Duration::seconds(2));
        assert_eq!(scheduler.try(), Some(Err(WaitError::Overrun(vec![0, 1]))));
        assert_eq!(scheduler.try(), Some(Ok(vec![2])));
    }
}
