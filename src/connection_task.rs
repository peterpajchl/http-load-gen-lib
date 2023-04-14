use anyhow::{Context, Ok, Result};
use async_trait::async_trait;
use hyper::{client::HttpConnector, http::Uri, Body, Client, StatusCode};
use tokio::sync::mpsc::{UnboundedSender};
use std::{time::{Duration, Instant}, collections::HashMap};
use super::notification::NotificationTask;
use serde::{Serialize, Deserialize};

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

#[derive(Serialize, Deserialize)]
pub struct Statistics {
    pub code: u16,
    pub requests: u64,
    pub max: f64,
    pub min: f64,
    pub mean: f64,
    pub sd: f64,
    pub p90: f64,
    pub p99: f64
}

impl Statistics {

    fn new(code: u16, requests: u64, max: f64, min: f64, mean: f64, sd: f64, p90: f64, p99: f64) -> Self {
        Statistics { code, requests, max, min, mean, sd, p90, p99 }
    }
}

#[derive(Debug, Eq, PartialOrd, Clone)]
pub struct RequestMetrics {
    pub latency: Duration,
    pub status_code: u16
}

impl std::cmp::Ord for RequestMetrics {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.latency.cmp(&other.latency)
    }
}

impl PartialEq for RequestMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.latency == other.latency
    }
}

pub struct Report {
    requests_metrics: Vec<RequestMetrics>,
    elapsed: Duration
}

impl Report {

    pub fn get_requests_count(&self) -> usize {
        self.requests_metrics.len()
    }

    pub fn get_successful_requests(&self) -> usize {
        self.requests_metrics.iter().filter(|&req| req.status_code >= 200 && req.status_code <= 418).collect::<Vec<&RequestMetrics>>().len()
    }

    pub fn get_failed_requests(&self) -> usize {
        self.requests_metrics.iter().filter(|&req| req.status_code > 418).collect::<Vec<&RequestMetrics>>().len()
    }

    pub fn get_elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn get_stats(&self) -> Vec<Statistics> {
        let mut map: HashMap<u16, Vec<RequestMetrics>> = HashMap::new();

        for req in self.requests_metrics.iter() {
            if map.contains_key(&req.status_code) {
                if let Some(data) = map.get_mut(&req.status_code) {
                    data.push(req.clone());
                }
            } else {
                map.insert(req.status_code, vec![req.clone()]);
            }
        }

        let mut statistics = Vec::with_capacity(map.keys().len());

        for (code, metrics) in &map {
            let mut values = metrics.iter().map(|x| x.latency.as_secs_f64()).collect::<Vec<f64>>();
            let max_ms = calc_max(values.clone()) * 1_000_000_f64;
            let min_ms = calc_min(values.clone()) * 1_000_000_f64;
            let mean = calc_mean(&values);
            let mean_ms = mean * 1_000_000_f64;
            let sd_ms = calc_sd(&mean, &values) * 1_000_000_f64;
            let p90 = calc_px(0.9, &mut values) * 1_000_000_f64;
            let p99 = calc_px(0.99, &mut values) * 1_000_000_f64;

            statistics.push(Statistics::new(code.clone(), metrics.len() as u64, max_ms, min_ms, mean_ms, sd_ms, p90, p99));
        }

        statistics
    }

    pub fn save_to_file(&self, file_name: String) -> Result<(), anyhow::Error> {
        let path = std::path::Path::new(&file_name);
        let mut wtr = csv::Writer::from_path(path)?;
        for stat in self.get_stats().iter() {
            wtr.serialize(stat)?;
        }
        wtr.flush()?;
        Ok(())
    }

}

fn calc_min(values: Vec<f64>) -> f64 {
    match values.into_iter().reduce(f64::min) {
        Some(x) => x,
        None => 0.0
    }
}

fn calc_max(values: Vec<f64>) -> f64 {
    match values.into_iter().reduce(f64::max) {
        Some(x) => x,
        None => 0.0
    }
}

fn calc_mean(values: &Vec<f64>) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn calc_sd(mean: &f64, values: &Vec<f64>) -> f64 {
    let sum = values.iter().map(|v| (v - mean).powf(2_f64)).sum::<f64>();
    (sum / values.len() as f64).sqrt()
}

fn calc_px(percentile: f64, values: &mut Vec<f64>) -> f64 {
    let p90_index = (values.len() as f64 * percentile).ceil() as usize;
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[p90_index - 1]
}

pub async fn task(
    connector: impl HTTPClientConformer,
    task_props: TaskProps,
) -> Result<Vec<RequestMetrics>, anyhow::Error> {
    let mut metrics = Vec::with_capacity(task_props.num_of_requests as usize);

    for i in 0..task_props.num_of_requests {
        let now = Instant::now();
        let status_code = connector.fetch(task_props.uri.clone()).await?.as_u16();
        let latency = now.elapsed();
        let request_metric = RequestMetrics { latency,  status_code };

        metrics.push(request_metric);

        if i % 250 == 0 {
            let msg = NotificationTask {
                connection_id: task_props.task_id,
                progress_count: i
            };

            if let Some(ref sender) = task_props.tx {
                let _ = sender.send(msg);
            }
        }
    }

    Ok(metrics)

}

pub async fn run(settings: RunSettings, messenger: Option<UnboundedSender<NotificationTask>>) -> Result<Report> {
    let mut join_handles = Vec::with_capacity(settings.connections as usize);
    let mut task_results = Vec::with_capacity(settings.connections as usize);
    let mut request_overflow = settings.requests % settings.connections as u64;
    let now = Instant::now();

    for i in 0..settings.connections {
        let client = Client::builder().build_http::<Body>();

        let addition: u64 = match request_overflow {
            x if x >= 1 => {
                request_overflow -= 1;
                1
            },
            _ => 0
        };

        let task_props = TaskProps::new(
            messenger.clone(),
            i as u64,
            (settings.requests / settings.connections as u64) + addition,
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

    let elapsed = now.elapsed();

    let requests_metrics = task_results.into_iter().flatten().collect::<Vec<RequestMetrics>>();

    Ok(Report { requests_metrics, elapsed })

}


#[cfg(test)]
mod test_connection {
    use std::vec;
    use tokio::time::Duration;
    use async_trait::async_trait;
    use hyper::{StatusCode, Uri};
    use crate::connection_task::TaskProps;
    use super::{HTTPClientConformer, Report, RequestMetrics, task, calc_min, calc_max, calc_mean, calc_sd, calc_px};

    struct MockClient {
        response_code: Option<StatusCode>
    }

    impl MockClient {
        fn response_with_code(response_code: Option<StatusCode>) -> Self {
            MockClient { response_code }
        }
    }

    #[test]
    fn test_save_to_file() {
        let report = Report {
            requests_metrics: vec![
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 200 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 200 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 200 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 200 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 200 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 200 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 404 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 418 },
                RequestMetrics { latency: Duration::from_micros(90) , status_code: 500 }
            ],
            elapsed: Duration::from_millis(5)
        };

        let file_name = String::from("test.csv");
        let result = report.save_to_file(file_name);
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_calc_min() {
        let test_values: Vec<f64> = vec![6.0, 2.0, 3.0, 1.0];
        let min = calc_min(test_values);
        assert_eq!(min, 1.0);
    }

    #[test]
    fn test_calc_max() {
        let test_values: Vec<f64> = vec![6.0, 2.0, 3.0, 1.0];
        let max = calc_max(test_values);
        assert_eq!(max, 6.0);
    }

    #[test]
    fn test_calc_mean() {
        let test_values: Vec<f64> = vec![6.0, 2.0, 3.0, 1.0];
        let mean = calc_mean(&test_values);
        assert_eq!(mean, 3.0);
    }

    #[test]
    fn test_calc_sd() {
        let test_values: Vec<f64> = vec![6.0, 2.0, 3.0, 1.0];
        let mean = calc_mean(&test_values);
        let sd = calc_sd(&mean, &test_values);
        assert_eq!(sd, 1.8708286933869707);
    }

    #[test]
    fn test_calc_px() {
        let mut test_values: Vec<f64> = vec![6.0, 2.0, 3.0, 1.0];
        let p90 = calc_px(0.9, &mut test_values);
        let p99 = calc_px(0.99, &mut test_values);
        assert_eq!(p90, 6.0);
        assert_eq!(p99, 6.0);
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
        assert_eq!(stats.iter().filter(|x| x.status_code == 500).count(), 0);
        assert_eq!(stats.iter().filter(|x| x.status_code <= 418).count(), 12);
    }

    #[tokio::test]
    async fn test_bad_result() {
        let client = MockClient::response_with_code(Some(StatusCode::INTERNAL_SERVER_ERROR));
        let task_props = TaskProps::new(None, 0, 12, Uri::from_static("http://test.com/hello/world"));
        let result = task(client, task_props).await;
        let stats = result.expect("all requests should fail with code 500");
        assert_eq!(stats.iter().filter(|x| x.status_code == 500).count(), 12);
        assert_eq!(stats.iter().filter(|x| x.status_code <= 418).count(), 0);
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