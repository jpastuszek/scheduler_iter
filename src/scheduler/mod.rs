mod wait;
mod abortable_wait;

use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::fmt;
use std::cmp::PartialEq;
use time::Duration;

use time_source::*;
use steady_time_source::*;
use task::*;

type PointInTime = u64;

enum SchedulerAction {
    None,
    Wait(Duration),
    Skip(Vec<PointInTime>),
    Yield(PointInTime)
}

pub enum Schedule<Token> {
    NextIn(Duration),
    Overrun(Vec<Token>),
    Current(Vec<Token>)
}

impl<Token> PartialEq for Schedule<Token> where Token: PartialEq<Token> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&Schedule::NextIn(ref duration), &Schedule::NextIn(ref other_duration)) => duration == other_duration,
            (&Schedule::Overrun(ref tokens), &Schedule::Overrun(ref other_tokens)) => tokens == other_tokens,
            (&Schedule::Current(ref tokens), &Schedule::Current(ref other_tokens)) => tokens == other_tokens,
            _ => false
        }
    }
}

impl<Token> fmt::Debug for Schedule<Token> where Token: fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Schedule::NextIn(ref duration) => write!(f, "Schedule::NextIn({}ms)", duration.num_milliseconds()),
            &Schedule::Overrun(ref tokens) => write!(f, "Schedule::Overrun({:?})", tokens),
            &Schedule::Current(ref tokens) => write!(f, "Schedule::Current({:?})", tokens),
        }
    }
}

pub struct Scheduler<Token, TS> where TS: TimeSource, Token: Clone {
    time_point_interval: Duration,
    tasks: BTreeMap<PointInTime, Vec<Task<Token>>>,
    time_source: TS
}

impl<Token> Scheduler<Token, SteadyTimeSource> where Token: Clone {
    pub fn new(time_point_interval: Duration) -> Scheduler<Token, SteadyTimeSource> {
        Scheduler::with_time_source(time_point_interval, SteadyTimeSource::new())
    }
}

impl<Token, TS> Scheduler<Token, TS> where TS: TimeSource, Token: Clone {
    pub fn with_time_source(time_point_interval: Duration, time_source: TS) -> Scheduler<Token, TS> {
        assert!(time_point_interval > Duration::seconds(0), "time_point_interval must be positive Duration");
        Scheduler {
            time_point_interval: time_point_interval,
            tasks: BTreeMap::new(),
            time_source: time_source
        }
    }

    fn schedule(&mut self, task: Task<Token>) {
        let time_point = self.to_time_point(task.schedule());
        self.tasks.entry(time_point).or_insert(Vec::new()).push(task);
    }

    pub fn after(&mut self, duration: Duration, token: Token) {
        let task = Task::new(duration, self.time_source.now(), TaskBond::OneOff, token);
        self.schedule(task);
    }

    pub fn every(&mut self, duration: Duration, token: Token) {
        let task = Task::new(duration, self.time_source.now(), TaskBond::Perpetual, token);
        self.schedule(task);
    }

    fn next_action(&self) -> SchedulerAction {
        let now = self.time_source.now();
        let current_time_point = self.to_time_point(now);

        match self.tasks.iter().next() {
            None => SchedulerAction::None,
            Some((&time_point, _)) => {
                match time_point.cmp(&current_time_point) {
                    Ordering::Greater => SchedulerAction::Wait((self.to_duration(time_point)) - now),
                    Ordering::Less => SchedulerAction::Skip(self.tasks.iter().take_while(|&(&time_point, &_)| time_point < current_time_point).map(|(time_point, _)| time_point.clone()).collect()),
                    Ordering::Equal => SchedulerAction::Yield(time_point)
                }
            }
        }
    }

    pub fn next(&mut self) -> Option<Schedule<Token>> {
        match self.next_action() {
            SchedulerAction::None => None,
            SchedulerAction::Wait(duration) => {
                Some(Schedule::NextIn(duration))
            },
            SchedulerAction::Skip(time_points) => {
                let mut overrun = Vec::new();

                overrun.extend(self.consume(time_points));
                // collect all reschedules of consumed tasks if they end up overrun already
                while let SchedulerAction::Skip(time_points) = self.next_action() {
                    overrun.extend(self.consume(time_points));
                }
                Some(Schedule::Overrun(overrun))
            },
            SchedulerAction::Yield(time_point) => {
                Some(Schedule::Current(self.consume(vec![time_point])))
            }
        }
    }

    pub fn next_in(&self) -> Duration {
        match self.next_action() {
            SchedulerAction::Wait(duration) => duration,
            _ => Duration::zero()
        }
    }

    pub fn cancel(&mut self, token: &Token) where Token: PartialEq<Token> {
        let mut empty_time_points = vec![];

        for (ref time_point, ref mut tasks) in self.tasks.iter_mut() {
            tasks.retain(|task| task.token != *token);
            if tasks.is_empty() {
                empty_time_points.push(*time_point.clone());
            }
        }

        for time_point in empty_time_points {
            self.tasks.remove(&time_point).unwrap();
        }
    }

    fn consume(&mut self, time_points: Vec<PointInTime>) -> Vec<Token> {
        let mut tasks: Vec<Task<Token>> = time_points.iter().flat_map(|time_point|
                self.tasks.remove(&time_point).unwrap()
            ).collect();

        tasks.sort_by(|a, b| a.run_offset.cmp(&b.run_offset));
        let tokens = tasks.iter().map(|ref task| task.token.clone()).collect();

        for task in tasks {
            match task.bond {
                TaskBond::Perpetual => self.schedule(task.next()),
                TaskBond::OneOff => ()
            };
        }
        tokens
    }

    fn to_time_point(&self, duration: Duration) -> PointInTime {
        // nanoseconds gives 15250 weeks or 299 years of duration max... should do?
        let interval = self.time_point_interval.num_nanoseconds().expect("interval too large");
        let duration = duration.num_nanoseconds().expect("duration too large");
        assert!(duration >= 0);

        (duration / interval) as PointInTime
    }

    fn to_duration(&self, time_point: PointInTime) -> Duration {
        Duration::nanoseconds(self.time_point_interval.num_nanoseconds().expect("time point interval too large") * time_point as i64)
    }
}

impl<Token, TS> FastForward for Scheduler<Token, TS> where TS: TimeSource + FastForward, Token: Clone {
    fn fast_forward(&mut self, duration: Duration) {
        self.time_source.fast_forward(duration);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use time_source::*;
    use test_helpers::*;
    use time::Duration;

    #[test]
    fn to_time_point() {
        let scheduler: Scheduler<(), _> = Scheduler::new(Duration::seconds(1));
        assert_eq!(scheduler.to_time_point(Duration::seconds(0)), 0);
        assert_eq!(scheduler.to_time_point(Duration::seconds(1)), 1);
        assert_eq!(scheduler.to_time_point(Duration::seconds(2)), 2);
        assert_eq!(scheduler.to_time_point(Duration::milliseconds(2000)), 2);
        assert_eq!(scheduler.to_time_point(Duration::milliseconds(100)), 0);
        assert_eq!(scheduler.to_time_point(Duration::milliseconds(1100)), 1);
        assert_eq!(scheduler.to_time_point(Duration::milliseconds(1500)), 1);
        assert_eq!(scheduler.to_time_point(Duration::milliseconds(1800)), 1);
        assert_eq!(scheduler.to_time_point(Duration::milliseconds(2800)), 2);
    }

    #[test]
    fn to_time_point_limits() {
        let scheduler: Scheduler<(), _> = Scheduler::new(Duration::nanoseconds(1));
        assert_eq!(scheduler.to_time_point(Duration::seconds(0)), 0);

        let scheduler: Scheduler<(), _> = Scheduler::new(Duration::weeks(15250) / 2);
        assert_eq!(scheduler.to_time_point(Duration::weeks(0)), 0);
        assert_eq!(scheduler.to_time_point(Duration::weeks(15250) / 2), 1);
        assert_eq!(scheduler.to_time_point(Duration::weeks(15250)), 2);
    }

    #[test]
    fn to_duration() {
        let scheduler: Scheduler<(), _> = Scheduler::new(Duration::seconds(1));
        assert_eq!(scheduler.to_duration(0), Duration::seconds(0));
        assert_eq!(scheduler.to_duration(1), Duration::seconds(1));
        assert_eq!(scheduler.to_duration(2), Duration::seconds(2));
    }

    #[test]
    fn to_duration_limits() {
        let scheduler: Scheduler<(), _> = Scheduler::new(Duration::nanoseconds(1));
        assert_eq!(scheduler.to_duration(0), Duration::nanoseconds(0));
        assert_eq!(scheduler.to_duration(1), Duration::nanoseconds(1));

        let scheduler: Scheduler<(), _> = Scheduler::new(Duration::weeks(15250) / 2);
        assert_eq!(scheduler.to_duration(0), Duration::seconds(0));
        assert_eq!(scheduler.to_duration(1), Duration::weeks(15250) / 2);
        assert_eq!(scheduler.to_duration(2), Duration::weeks(15250));
    }

    #[test]
    fn empty() {
        let mut scheduler: Scheduler<(), _> = Scheduler::new(Duration::seconds(1));
        assert_eq!(scheduler.next(), None);
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn after() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(0), 0);
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![0])));

        scheduler.after(Duration::seconds(1), 1);
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::seconds(1))));
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::seconds(1))));

        scheduler.fast_forward(Duration::milliseconds(100));
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::milliseconds(900))));
        scheduler.fast_forward(Duration::milliseconds(900));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![1])));
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn after_opaque_token() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(0), Zero);
        match scheduler.next().unwrap() {
            Schedule::Current(tokens) => {
                assert_eq!(tokens.len(), 1);
                tokens.first().unwrap().expect(Zero)
            }
            _ => panic!("expected Schedule::Current")
        }
    }

    #[test]
    fn every() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.every(Duration::seconds(1), 1);
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::seconds(1))));
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::seconds(1))));

        scheduler.fast_forward(Duration::milliseconds(100));
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::milliseconds(900))));
        scheduler.fast_forward(Duration::milliseconds(900));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![1])));
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::seconds(1))));

        scheduler.fast_forward(Duration::milliseconds(600));
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::milliseconds(400))));
        scheduler.fast_forward(Duration::milliseconds(500));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![1])));
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::milliseconds(900))));
    }

    #[test]
    fn every_with_overrun() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.every(Duration::seconds(1), 1);
        scheduler.fast_forward(Duration::seconds(4));
        assert_eq!(scheduler.next(), Some(Schedule::Overrun(vec![1, 1, 1])));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![1])));
    }

    #[test]
    fn limits() {
        let mut scheduler = Scheduler::with_time_source(Duration::nanoseconds(1), MockTimeSource::new());

        scheduler.after(Duration::nanoseconds(1), 1);
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::nanoseconds(1))));

        scheduler.fast_forward(Duration::nanoseconds(1));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![1])));

        scheduler.after(Duration::weeks(15250), 2);
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::weeks(15250))));

        scheduler.fast_forward(Duration::weeks(15250));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![2])));

        let mut scheduler = Scheduler::with_time_source(Duration::weeks(15250) / 2, MockTimeSource::new());

        scheduler.after(Duration::weeks(15250) / 2, 1);
        assert_eq!(scheduler.next(), Some(Schedule::NextIn(Duration::weeks(15250) / 2)));

        scheduler.fast_forward(Duration::weeks(15250) / 2);
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![1])));
    }

    #[test]
    fn next_in() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(0), 0);
        assert_eq!(scheduler.next_in(), Duration::zero());
        scheduler.next();

        scheduler.fast_forward(Duration::seconds(1));
        scheduler.after(Duration::seconds(1), 1);
        assert_eq!(scheduler.next_in(), Duration::seconds(1));
        scheduler.next();

        scheduler.fast_forward(Duration::seconds(2));
        assert_eq!(scheduler.next_in(), Duration::zero());
    }

    #[test]
    fn cancel_whole_time_point() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(0), 0);
        scheduler.after(Duration::seconds(1), 1);
        scheduler.after(Duration::seconds(2), 2);
        scheduler.after(Duration::seconds(3), 4);

        scheduler.cancel(&1);
        scheduler.cancel(&2);

        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![0])));
        scheduler.fast_forward(Duration::seconds(3));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![4])));
    }

    #[test]
    fn cancel_single_token() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(0), 0);
        scheduler.after(Duration::seconds(1), 1);
        scheduler.after(Duration::seconds(1), 2);
        scheduler.after(Duration::seconds(1), 3);
        scheduler.after(Duration::seconds(2), 4);
        scheduler.after(Duration::seconds(2), 5);

        scheduler.cancel(&1);
        scheduler.cancel(&4);

        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![0])));
        scheduler.fast_forward(Duration::seconds(1));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![2, 3])));
        scheduler.fast_forward(Duration::seconds(1));
        assert_eq!(scheduler.next(), Some(Schedule::Current(vec![5])));
    }
}
