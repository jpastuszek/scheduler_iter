use time::{SteadyTime, Duration};
use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;
use std::cmp::PartialEq;

#[derive(Clone)]
struct Task<Token> where Token: Clone {
    interval: Duration,
    run_offset: Duration,
    token: Token,
    bond: TaskBond
}

#[derive(Clone, Debug)]
enum TaskBond {
    OneOff,
    Perpetual
}

impl<Token> fmt::Debug for Task<Token> where Token: Clone {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.bond {
            TaskBond::Perpetual => write!(f, "Task(every {}ms)", self.interval.num_milliseconds()),
            TaskBond::OneOff => write!(f, "Task(after {}ms)", self.interval.num_milliseconds())
        }
    }
}

impl<Token> Task<Token> where Token: Clone {
    fn new(interval: Duration, run_offset: Duration, bond: TaskBond, token: Token) -> Task<Token> {
        assert!(interval > Duration::seconds(0)); // negative interval would make schedule go back in time!
        Task {
            interval: interval,
            run_offset: run_offset,
            bond: bond,
            token: token
        }
    }

    //TODO: OneOff task should not have this
    fn next(self) -> Task<Token> {
        Task {
            run_offset: self.run_offset + self.interval,
            .. self
        }
    }

    fn schedule(&self) -> Duration {
        self.run_offset + self.interval
    }
}

pub trait TimeSource {
    // Duration since this TimeSource was crated
    fn now(&self) -> Duration;
}

pub trait FastForward {
    fn fast_forward(&mut self, duration: Duration);
}

pub struct SteadyTimeSource {
    offset: SteadyTime
}

impl SteadyTimeSource {
    fn new() -> SteadyTimeSource {
        SteadyTimeSource {
            offset: SteadyTime::now()
        }
    }
}

impl TimeSource for SteadyTimeSource {
    fn now(&self) -> Duration {
        SteadyTime::now() - self.offset
    }
}

// TODO: should that be u64?
type TimePoint = u32;

pub enum SchedulerAction {
    None,
    Wait(Duration),
    Skip(Vec<TimePoint>),
    Yield(TimePoint)
}

pub enum Schedule<Token> {
    NextIn(Duration),
    Missed(Vec<Token>),
    Current(Vec<Token>)
}

impl<Token> PartialEq for Schedule<Token> where Token: PartialEq<Token> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            &Schedule::NextIn(ref duration) => if let &Schedule::NextIn(ref other_duration) = other {
                duration == other_duration
            } else {
                false
            },
            &Schedule::Missed(ref tokens) => if let &Schedule::Missed(ref other_tokens) = other {
                tokens == other_tokens
            } else {
                false
            },
            &Schedule::Current(ref tokens) => if let &Schedule::Current(ref other_tokens) = other {
                tokens == other_tokens
            } else {
                false
            }
        }
    }
}

impl<Token> Debug for Schedule<Token> where Token: Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Schedule::NextIn(ref duration) => write!(f, "Schedule::NextIn({}ms)", duration.num_milliseconds()),
            &Schedule::Missed(ref tokens) => write!(f, "Schedule::Missed({:?})", tokens),
            &Schedule::Current(ref tokens) => write!(f, "Schedule::Current({:?})", tokens),
        }
    }
}

pub struct Scheduler<Token, TS> where TS: TimeSource, Token: Clone {
    time_point_interval: Duration,
    tasks: BTreeMap<TimePoint, Vec<Task<Token>>>,
    time_source: TS
}

impl<Token> Scheduler<Token, SteadyTimeSource> where Token: Clone {
    //TODO: way to specify how many durations it is OK to consider task current
    pub fn new(time_point_interval: Duration) -> Scheduler<Token, SteadyTimeSource> {
        Scheduler::with_time_source(time_point_interval, SteadyTimeSource::new())
    }

}

impl<Token, TS> Scheduler<Token, TS> where TS: TimeSource, Token: Clone {
    pub fn with_time_source(time_point_interval: Duration, time_source: TS) -> Scheduler<Token, TS> {
        assert!(time_point_interval > Duration::seconds(0));
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

    // TODO: we may miss some time points if wait waits longer
    fn next_action(&self) -> SchedulerAction {
        let now = self.time_source.now();
        let current_time_point = self.to_time_point(now);

        match self.tasks.iter().next() {
            None => SchedulerAction::None,
            Some((&time_point, _)) => {
                match time_point.cmp(&current_time_point) {
                    Ordering::Greater => SchedulerAction::Wait((self.to_duration(time_point)) - now),
                    //TODO: make task Eq for sorting by time
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
                Some(Schedule::Missed(self.consume(time_points)))
            },
            SchedulerAction::Yield(time_point) => {
                Some(Schedule::Current(self.consume(vec![time_point])))
            }
        }
    }

    fn consume(&mut self, time_points: Vec<TimePoint>) -> Vec<Token> {
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

    fn to_time_point(&self, duration: Duration) -> TimePoint {
        let interval = self.time_point_interval.num_microseconds().unwrap();
        let duration = duration.num_microseconds().unwrap();
        assert!(duration >= 0);

        (duration / interval) as TimePoint
    }

    fn to_duration(&self, time_point: TimePoint) -> Duration {
        self.time_point_interval * (time_point as i32)
    }
}

impl<Token, TS> FastForward for Scheduler<Token, TS> where TS: TimeSource + FastForward, Token: Clone {
    fn fast_forward(&mut self, duration: Duration) {
        self.time_source.fast_forward(duration);
    }
}

#[cfg(test)]
mod task {
    use super::*;
    use super::{Task, TaskBond};
    use time::Duration;

    struct MockTimeSource {
        current_time: Duration
    }

    impl MockTimeSource {
        fn new() -> MockTimeSource {
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

    #[test]
    fn getting_next_schedule() {
        let now = Duration::seconds(0);
        let interval = Duration::seconds(1);
        let task = Task::new(interval, now, TaskBond::OneOff, 42);
        println!("task: {:?}", task);
        assert_eq!(task.schedule(), now + interval);
        assert_eq!(task.next().next().schedule(), now + interval * 3);
    }

    #[test]
    fn scheduler_to_time_point() {
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
    fn scheduler_after() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.after(Duration::seconds(1), 1i32);
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::seconds(1))));
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::seconds(1))));

        scheduler.fast_forward(Duration::milliseconds(100));
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::milliseconds(900))));
        scheduler.fast_forward(Duration::milliseconds(900));
        assert_eq!(scheduler.next(), Option::Some(Schedule::Current(vec![1])));
        assert_eq!(scheduler.next(), Option::None);
    }

    #[test]
    fn scheduler_every() {
        let mut scheduler = Scheduler::with_time_source(Duration::seconds(1), MockTimeSource::new());

        scheduler.every(Duration::seconds(1), 1i32);
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::seconds(1))));
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::seconds(1))));

        scheduler.fast_forward(Duration::milliseconds(100));
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::milliseconds(900))));
        scheduler.fast_forward(Duration::milliseconds(900));
        assert_eq!(scheduler.next(), Option::Some(Schedule::Current(vec![1])));
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::seconds(1))));

        scheduler.fast_forward(Duration::milliseconds(600));
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::milliseconds(400))));
        scheduler.fast_forward(Duration::milliseconds(500));
        assert_eq!(scheduler.next(), Option::Some(Schedule::Current(vec![1])));
        assert_eq!(scheduler.next(), Option::Some(Schedule::NextIn(Duration::milliseconds(900))));
    }
}

