use std::hint;
use futures_util::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use anyhow::Error;
use dashmap::DashSet;
use sqlx::{Connection, Either, PgConnection, Postgres, query};
use sqlx::postgres::{PgQueryResult, PgRow};
use sqlx::types::Uuid;
use tokio::sync::Mutex;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::sleep;

lazy_static::lazy_static!{
    static ref PATH: PathBuf = std::env::var("COUNTER_PATH").expect("COUNTER_PATH must be set").into();
}

pub(crate) struct Counter {
    local_count: AtomicI64,
    global_count: AtomicI64,
    uncertain: AtomicBool,
    reading: AtomicUsize,
    known: DashSet<Uuid>,
    db: Mutex<Option<PgConnection>>,
    jh: Mutex<Option<JoinHandle<anyhow::Result<()>>>>,
    alert: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}


#[derive(Debug)]
struct Record {
    count: Uuid,
    by_val: i64,
}

impl Counter {
    pub async fn new() -> anyhow::Result<(Self, Sender<()>)> {
        let num =
            if PATH.exists() {
                eprintln!("Recovering lost counter");

                let num = tokio::fs::read_to_string(&*PATH).await?;
                let res = num.parse::<i64>().ok();

                tokio::fs::remove_file(&*PATH).await?;

                if res.is_none() {
                    eprintln!("Failed to parse counter");
                }

                res
            } else {
                None
            }.unwrap_or(0);

        let mut db = PgConnection::connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")).await?;

        let mut iter = sqlx::query_as!(Record, "SELECT count, by_val FROM public.counts").fetch_many(&mut db);

        let known = DashSet::new();
        let mut counter = 0;
        while let Some(row) = iter.next().await {
            match row? {
                Either::Left(res) => (),
                Either::Right(row) => {
                    if known.insert(row.count) {
                        counter += row.by_val;
                    }
                }
            }
        }

        drop(iter);

        let (tx, rx) = tokio::sync::oneshot::channel();

        Ok((Self {
            local_count: AtomicI64::new(num),
            global_count: AtomicI64::new(counter),
            uncertain: Default::default(),
            reading: Default::default(),
            known,
            db: Mutex::new(Some(db)),
            jh: Default::default(),
            alert: Mutex::new(Some(rx))
        }, tx))
    }

    fn start_read(&self) {
        self.reading.fetch_add(1, Ordering::Release);
        while self.uncertain.load(Ordering::Acquire) {
            hint::spin_loop();
        }
    }

    fn end_read(&self) {
        self.reading.fetch_sub(1, Ordering::Release);
    }

    pub fn inc(&self) -> i64 {
        self.start_read();
        let res = self.local_count.fetch_add(1, Ordering::Relaxed) + self.global_count.load(Ordering::Relaxed);
        self.end_read();
        res
    }

    pub fn dec(&self) -> i64 {
        self.start_read();
        let res = self.local_count.fetch_sub(1, Ordering::Relaxed) + self.global_count.load(Ordering::Relaxed);
        self.end_read();
        res
    }

    pub fn get(&self) -> i64 {
        self.start_read();
        let res = self.local_count.load(Ordering::Relaxed) + self.global_count.load(Ordering::Relaxed);
        self.end_read();
        res
    }

    pub async fn wrap_up(&self) -> anyhow::Result<()> {
        let res = self.jh.lock().await.as_mut().ok_or(Error::msg(""))?.await?;
        *self.jh.lock().await = None;
        res
    }

    pub async fn start_saving(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut db = self.db.lock().await.take().expect("Can't call start_saving twice");
        let mut signal = self.alert.lock().await.take().expect("Can't call start_saving twice");

        let this = self.clone();
        *self.jh.lock().await = Some(tokio::spawn(async move {
            let mut done = false;
            while !done {
                let n = tokio::select! {
                    n = interval_check(&*this, 10_000_000) => n,
                    _ = sleep(Duration::from_secs(5 * 60)) => {
                        this.uncertain.store(true, Ordering::Release);

                        while this.reading.load(Ordering::Acquire) != 0 {
                            std::hint::spin_loop();
                        };

                        let n = this.local_count.swap(0, Ordering::AcqRel);
                        this.global_count.fetch_add(n, Ordering::Release);

                        this.uncertain.store(false, Ordering::Release);

                        n
                    },
                    _ = &mut signal => {
                        done = true;
                        this.local_count.swap(0, Ordering::SeqCst)
                    },
                };
                if n == 0 {
                    continue;
                }
                sqlx::query!("INSERT INTO public.counts (by_val) VALUES ($1)", n).execute(&mut db).await?;
            }
            Ok(())
        }));

        async fn interval_check(counter: &Counter, threshold: u64) -> i64 {
            loop {
                if counter.local_count.load(Ordering::Relaxed).unsigned_abs() >= threshold {
                    let n = counter.local_count.swap(0, Ordering::Relaxed);
                    counter.global_count.fetch_add(n, Ordering::Relaxed);
                    return n;
                }
                sleep(Duration::from_secs(1)).await;
            }
        }

        Ok(())
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        let left = self.local_count.load(Ordering::SeqCst);
        if left != 0 {
            println!("ERROR: Dropping counter with non-zero count");
            if let Err(err) = std::fs::write(&*PATH, left.to_string()) {
                eprintln!("Failed to write counter: {}\n\t;c", err);
            }
        }
    }
}
