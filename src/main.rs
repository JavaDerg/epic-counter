use std::sync::Arc;
use axum::Extension;
use axum::routing::get;
use crate::counter::Counter;

mod counter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (counter, wakeup)
        = counter::Counter::new().await?;

    let counter = Arc::new(counter);

    counter.start_saving().await?;

    let service = axum::Router::new()
        .route("/get", get(get_count))
        .route("/inc", get(inc))
        .route("/dec", get(dec))
        .layer(Extension(counter.clone()))
        .into_make_service();

    let server =
        axum::Server::bind(&"0.0.0.0:1337".parse().unwrap())
            .serve(service);

    tokio::select! {
        err = server => {
            if let Err(e) = err {
                eprintln!("server error: {}", e);
            }
        },
        _ = tokio::signal::ctrl_c() => (),
        err = counter.wrap_up() => {
            if let Err(e) = err {
                eprintln!("counter error: {}", e);
            }
        },
    }

    let _ = wakeup.send(());

    counter.wrap_up().await?;

    Ok(())
}

async fn inc(Extension(counter): Extension<Arc<Counter>>) -> String {
    counter.inc().to_string()
}

async fn dec(Extension(counter): Extension<Arc<Counter>>) -> String {
    counter.dec().to_string()
}

async fn get_count(Extension(counter): Extension<Arc<Counter>>) -> String {
    counter.get().to_string()
}
