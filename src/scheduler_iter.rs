use time::{SteadyTime, Duration};
use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;

#[derive(Clone)]
struct Task<Token> where Token: Clone {
    interval: Duration,
    run_offset: SteadyTime,
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
    fn new(interval: Duration, run_offset: SteadyTime, bond: TaskBond, token: Token) -> Task<Token> {
        assert!(interval > Duration::seconds(0)); // negative interval would make schedule go back in time!
        Task {
            interval: interval,
            run_offset: run_offset,
            bond: bond,
            token: token
        }
    }

    // TODO: should that return new task (immutable?)
    fn next_schedule(&mut self) -> SteadyTime {
        self.run_offset = self.run_offset + self.interval;
        self.run_offset // ?
    }
}

pub trait TimeSource {
    fn now(&self) -> SteadyTime;
    fn wait(&mut self, duration: Duration);
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
    Missed(Vec<Token>),
    Current(Vec<Token>)
}

pub struct Scheduler<Token, TS> where TS: TimeSource, Token: Clone {
    offset: SteadyTime,
    time_point_interval: Duration,
    tasks: BTreeMap<TimePoint, Vec<Task<Token>>>,
    time_source: TS
}

impl<Token, TS> Scheduler<Token, TS> where TS: TimeSource, Token: Clone {
    pub fn new(time_point_interval: Duration, time_source: TS) -> Scheduler<Token, TS> where TS: TimeSource {
        Scheduler {
            offset: time_source.now(),
            time_point_interval: time_point_interval,
            tasks: BTreeMap::new(),
            time_source: time_source
        }
    }

    fn schedule(&mut self, mut task: Task<Token>) {
        let now = self.time_source.now();
        assert!(now > self.offset);
        let current_time_point = self.to_time_point(now - self.offset);

        //TODO: optimise
        //TODO: this should not loop - just schedule for next and let scheduler to report Missed
        let mut time_point;
        loop {
            time_point = self.to_time_point(task.next_schedule() - now);
            if time_point > current_time_point {
                break;
            }
        }

        self.tasks.entry(time_point).or_insert(Vec::new()).push(task);
    }

    pub fn after(&mut self, duration: Duration, token: Token) {
        self.schedule(Task::new(duration, SteadyTime::now(), TaskBond::OneOff, token));
    }

    pub fn every(&mut self, duration: Duration, token: Token) {
        self.schedule(Task::new(duration, SteadyTime::now(), TaskBond::Perpetual, token));
    }

    // TODO: we may miss some time points if wait waits longer
    fn next_action(&self) -> SchedulerAction {
        let now = self.time_source.now();
        let current_time_point = self.to_time_point(now - self.offset);

        match self.tasks.iter().next() {
            None => SchedulerAction::None,
            Some((&time_point, _)) => {
                match time_point.cmp(&current_time_point) {
                    Ordering::Greater => SchedulerAction::Wait((self.offset + self.to_duration(time_point)) - now),
                    //TODO: make task Eq for sorting by time
                    Ordering::Less => SchedulerAction::Skip(self.tasks.iter().take_while(|&(&time_point, &_)| time_point < current_time_point).map(|(time_point, _)| time_point.clone()).collect()),
                    Ordering::Equal => SchedulerAction::Yield(time_point)
                }
            }
        }
    }

    pub fn wait(&mut self) -> Option<Schedule<Token>> {
        match self.next_action() {
            SchedulerAction::None => None,
            SchedulerAction::Wait(duration) => {
                self.time_source.wait(duration);
                self.wait()
            },
            SchedulerAction::Skip(time_points) => {
                let tasks: Vec<Task<Token>> = time_points.iter().flat_map(|time_point|
                        self.tasks.remove(&time_point).unwrap()
                    ).collect();
                for task in tasks.iter() {
                    match task.bond {
                        TaskBond::Perpetual => self.schedule(task.clone()),
                        TaskBond::OneOff => ()
                    };
                }
                Some(Schedule::Missed(tasks.into_iter().map(|task| task.token).collect()))
            },
            SchedulerAction::Yield(time_point) => {
                let mut tasks = self.tasks.remove(&time_point).unwrap();
                for task in tasks.iter() {
                    match task.bond {
                        TaskBond::Perpetual => self.schedule(task.clone()),
                        TaskBond::OneOff => ()
                    };
                }

                tasks.sort_by(|a, b| a.run_offset.cmp(&b.run_offset));

                Some(Schedule::Current(tasks.into_iter().map(|task| task.token).collect()))
            }
        }
    }

    #[allow(dead_code)]
    pub fn time_source(&self) -> &TimeSource {
        &self.time_source
    }

    fn to_time_point(&self, duration: Duration) -> TimePoint {
        let interval = self.time_point_interval.num_microseconds().unwrap();
        let duration = duration.num_microseconds().unwrap();
        assert!(duration >= 0);

        let time_point = duration / interval;
        if duration % interval != 0 {
            time_point + 1; // ceil
        }
        time_point as TimePoint
    }

    fn to_duration(&self, time_point: TimePoint) -> Duration {
        self.time_point_interval * (time_point as i32)
    }
}

#[cfg(test)]
mod task {
    use time::{SteadyTime, Duration};
    use super::{Task, TaskBond};

    #[test]
    fn getting_next_schedule() {
        let now = SteadyTime::now();
        let interval = Duration::seconds(1);
        let mut task = Task::new(interval, now, TaskBond::OneOff, 42);
        println!("task: {:?}", task);
        assert_eq!(task.next_schedule(), now + interval);
        assert_eq!(task.next_schedule(), now + interval * 2);
        assert_eq!(task.next_schedule(), now + interval * 3);
    }
}

