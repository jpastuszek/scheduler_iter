use time::Duration;

#[derive(Clone)]
pub struct Task<Token> where Token: Clone {
    pub interval: Duration,
    pub run_offset: Duration,
    pub token: Token,
    pub bond: TaskBond
}

#[derive(Clone, Debug)]
pub enum TaskBond {
    OneOff,
    Perpetual
}

impl<Token> Task<Token> where Token: Clone {
    pub fn new(interval: Duration, run_offset: Duration, bond: TaskBond, token: Token) -> Task<Token> {
        assert!(interval >= Duration::seconds(0), "negative interval would make schedule go back in time!");
        Task {
            interval: interval,
            run_offset: run_offset,
            bond: bond,
            token: token
        }
    }

    pub fn next(self) -> Task<Token> {
        Task {
            run_offset: self.run_offset + self.interval,
            .. self
        }
    }

    pub fn schedule(&self) -> Duration {
        self.run_offset + self.interval
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use time::Duration;

    #[test]
    fn task_next_schedule() {
        let now = Duration::seconds(0);
        let interval = Duration::seconds(1);
        let task = Task::new(interval, now, TaskBond::OneOff, 42);

        assert_eq!(task.schedule(), now + interval);
        assert_eq!(task.next().next().schedule(), now + interval * 3);
    }
}
