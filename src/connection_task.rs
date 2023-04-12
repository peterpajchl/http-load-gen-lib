use anyhow::{Context, Ok, Result};
use async_trait::async_trait;
use hyper::{client::HttpConnector, http::Uri, Body, Client, StatusCode};
use tokio::sync::mpsc::{UnboundedSender};
use std::{time::{Duration, Instant}};
use super::notification::NotificationTask;

type HTTPClient = Client<HttpConnector, Body>;

#[async_trait]
pub trait HTTPClientConformer {
    async fn fetch(&self, uri: Uri) -> Result<StatusCode, anyhow::Error>;
}

#[async_trait]
impl HTTPClientConformer for HTTPClient {
    async fn fetch(&self, uri: Uri) -> Result<StatusCode, anyhow::Error> {
        let response = self.get(uri).await?;
        Ok(response.status())
    }
}

pub struct RunSettings {
    pub requests: u64,
    pub connections: u16,
    pub target_url: Uri
}

pub struct TaskProps {
    tx: Option<UnboundedSender<NotificationTask>>,
    task_id: u64,
    num_of_requests: u64,
    uri: Uri,
}

impl TaskProps {
    pub fn new(tx: Option<UnboundedSender<NotificationTask>>, task_id: u64, num_of_requests: u64, uri: Uri) -> Self {
        TaskProps {
            tx,
            task_id,
            num_of_requests,
            uri,
        }
    }
}

#[derive(Debug)]
pub struct ConnectionStats {
    pub connection_id: u64,
    pub requests_executed: u64,
    pub requests_succeded: u64,
    pub requests_failed: u64,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct TaskResults {
    pub stats_summaries: Vec<ConnectionStats>,
    pub total_duration_ms: u64,
}

impl ConnectionStats {
    pub fn new() -> Self {
        ConnectionStats {
            connection_id: 0,
            requests_executed: 0,
            requests_succeded: 0,
            requests_failed: 0,
            duration: Duration::ZERO,
        }
    }

    pub fn add(&mut self, stats: ConnectionStats) {
        self.requests_executed += stats.requests_executed;
        self.requests_succeded += stats.requests_succeded;
        self.requests_failed += stats.requests_failed;
        self.duration += stats.duration;
    }

    pub fn output_report(&self) {
        println!(
            "Total: {} Success: {} Failed: {} Elapsed: {} ms Rate: {} req/s",
            self.requests_executed,
            self.requests_succeded,
            self.requests_failed,
            self.duration.as_millis(),
            self.requests_executed * 1000 / self.duration.as_millis() as u64
        );
    }
}

pub async fn task(
    connector: impl HTTPClientConformer,
    task_props: TaskProps,
) -> Result<ConnectionStats, anyhow::Error> {
    let mut connection_stats = ConnectionStats::new();
    connection_stats.connection_id = task_props.task_id;

    for i in 0..task_props.num_of_requests {
        let now = Instant::now();
        let status_code = connector.fetch(task_props.uri.clone()).await?;
        let duration = now.elapsed();
        connection_stats.requests_executed += 1;
        connection_stats.duration += duration;

        if i % 250 == 0 {
            let msg = NotificationTask {
                connection_id: task_props.task_id,
                progress_count: i
            };

            if let Some(ref sender) = task_props.tx {
                let _ = sender.send(msg);
            }
        }

        match status_code {
            StatusCode::OK => connection_stats.requests_succeded += 1,
            _ => connection_stats.requests_failed += 1,
        }
    }

    Ok(connection_stats)
}

pub async fn run(settings: RunSettings, messenger: Option<UnboundedSender<NotificationTask>>) -> Result<TaskResults> {
    let mut join_handles = Vec::new();
    let mut task_results = Vec::new();
    let now = Instant::now();

    for i in 0..settings.connections {
        let client = Client::builder().build_http::<Body>();
        let task_props = TaskProps::new(
            messenger.clone(),
            i as u64,
            settings.requests / settings.connections as u64,
            settings.target_url.clone(),
        );

        join_handles.push(tokio::spawn(task(client, task_props)));
    }

    for handle in join_handles {
        let handle_result = handle.await;
        let connection_result = handle_result.context("Error: failed to resolve a task")?;
        let result = connection_result.context("Error: failure at the connection level")?;
        task_results.push(result);
    }

    let completion = now.elapsed().as_millis() as u64;

    let final_results = TaskResults {
        stats_summaries: task_results,
        total_duration_ms: completion,
    };

    Ok(final_results)

}


#[cfg(test)]
mod test_connection {
    use async_trait::async_trait;
    use hyper::{StatusCode, Uri};
    use crate::connection_task::TaskProps;
    use super::{HTTPClientConformer, task};

    struct MockClient {
        response_code: Option<StatusCode>
    }

    impl MockClient {
        fn response_with_code(response_code: Option<StatusCode>) -> Self {
            MockClient { response_code }
        }
    }

    #[async_trait]
    impl HTTPClientConformer for MockClient {
        async fn fetch(&self, _uri: Uri) -> Result<StatusCode, anyhow::Error> {
            match self.response_code {
                Some(code) => Ok(code),
                None => Err(anyhow::format_err!("Trouble"))
            }
        }
    }

    #[tokio::test]
    async fn test_ok_result() {
        let client = MockClient::response_with_code(Some(StatusCode::OK));
        let task_props = TaskProps::new(None, 0, 12, Uri::from_static("http://test.com/hello/world"));
        let result = task(client, task_props).await;
        let stats = result.expect("all requests should be successful with code 200");
        assert_eq!(stats.requests_failed, 0);
        assert_eq!(stats.requests_succeded, 12);
    }

    #[tokio::test]
    async fn test_bad_result() {
        let client = MockClient::response_with_code(Some(StatusCode::INTERNAL_SERVER_ERROR));
        let task_props = TaskProps::new(None, 0, 12, Uri::from_static("http://test.com/hello/world"));
        let result = task(client, task_props).await;
        let stats = result.expect("all requests should fail with code 500");
        assert_eq!(stats.requests_failed, 12);
        assert_eq!(stats.requests_succeded, 0);
    }

    #[tokio::test]
    async fn test_connection_failure_result() {
        let client = MockClient::response_with_code(None);
        let task_props = TaskProps::new(None, 0, 12, Uri::from_static("http://test.com/hello/world"));
        let result = task(client, task_props).await;
        let err = result.unwrap_err();
        assert_eq!(err.to_string(), "Trouble");
    }

}