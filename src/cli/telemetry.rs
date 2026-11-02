//! Logging and optional OpenTelemetry (OTLP) trace export.
//!
//! The OTLP exporter is only compiled in with the `telemetry` cargo feature
//! and only sends traces when `OTEL_EXPORTER_OTLP_ENDPOINT` is set at runtime.

use anyhow::Result;
#[cfg(feature = "telemetry")]
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
#[cfg(feature = "telemetry")]
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
#[cfg(feature = "telemetry")]
use opentelemetry_sdk::{
    Resource,
    trace::{SdkTracerProvider, Tracer},
};
#[cfg(feature = "telemetry")]
use std::{sync::OnceLock, time::Duration};
#[cfg(feature = "telemetry")]
use tonic::transport::ClientTlsConfig;
use tracing::Level;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

#[cfg(feature = "telemetry")]
static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Environment variable lookup matching `opentelemetry-otlp`'s `read_env_var`:
/// unset and empty values are both `None`, non-Unicode values are an error.
#[cfg(feature = "telemetry")]
fn read_env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "environment variable value is not valid Unicode"
        )),
    }
}

/// Resolve the OTLP traces endpoint exactly like `opentelemetry-otlp`'s tonic
/// exporter does: the signal-specific variable wins over the generic one, and a
/// schemeless endpoint gets `http://` or `https://` per the OTLP `INSECURE`
/// rule. The result is pinned on the exporter via `with_endpoint` so the TLS
/// wiring in [`init_tracer`] can never disagree with the endpoint being dialed.
#[cfg(feature = "telemetry")]
fn resolve_traces_endpoint() -> Result<String> {
    let endpoint = read_env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")?
        .or(read_env("OTEL_EXPORTER_OTLP_ENDPOINT")?)
        .unwrap_or_else(|| "http://localhost:4317".to_string());

    let insecure = std::env::var("OTEL_EXPORTER_OTLP_TRACES_INSECURE")
        .ok()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_INSECURE").ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));

    let has_scheme = endpoint
        .split_once("://")
        .is_some_and(|(scheme, _)| !scheme.contains(['/', '?', '#']));
    Ok(if has_scheme {
        endpoint
    } else if insecure {
        format!("http://{endpoint}")
    } else {
        format!("https://{endpoint}")
    })
}

#[cfg(feature = "telemetry")]
fn init_tracer() -> Result<Tracer> {
    // Only initialize OTLP if an endpoint is explicitly provided in the environment
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        return Err(anyhow::anyhow!("No OTLP endpoint configured"));
    }

    let endpoint = resolve_traces_endpoint()?;

    // opentelemetry-otlp's default HTTPS TLS config carries no trust roots, so
    // install the webpki roots explicitly to make `https://` endpoints work.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_timeout(Duration::from_secs(3))
        .with_endpoint(endpoint.clone());
    let is_https = endpoint
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("https"));
    let exporter = if is_https {
        exporter.with_tls_config(ClientTlsConfig::new().with_enabled_roots())
    } else {
        exporter
    };

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter.build()?)
        .with_resource(
            Resource::builder_empty()
                .with_attributes(vec![
                    KeyValue::new("service.name", env!("CARGO_PKG_NAME")),
                    KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                ])
                .build(),
        )
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer(env!("CARGO_PKG_NAME"));

    // Store the provider for later shutdown
    let _ = TRACER_PROVIDER.set(tracer_provider);

    Ok(tracer)
}

/// Start the telemetry layer
/// # Errors
/// Will return an error if the telemetry layer fails to start
pub fn init(verbosity_level: Option<Level>) -> Result<()> {
    let verbosity_level = verbosity_level.unwrap_or(Level::INFO);

    let fmt_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_file(false)
        .with_line_number(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_target(false)
        .with_level(false) // Hide "INFO" prefix
        .without_time()
        .compact();

    let filter = EnvFilter::builder()
        .with_default_directive(verbosity_level.into())
        .from_env_lossy()
        .add_directive("hyper=error".parse()?)
        .add_directive("tokio=error".parse()?)
        .add_directive("reqwest=error".parse()?);

    let registry = Registry::default().with(fmt_layer).with(filter);

    // The OTLP layer is only compiled in with the `telemetry` feature and only
    // attached when an endpoint is explicitly configured in the environment.
    #[cfg(feature = "telemetry")]
    if let Ok(tracer) = init_tracer() {
        let otel_tracer_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing::subscriber::set_global_default(registry.with(otel_tracer_layer))?;
        return Ok(());
    }

    tracing::subscriber::set_global_default(registry)?;

    Ok(())
}

/// Shutdown the telemetry layer
pub fn shutdown() {
    #[cfg(feature = "telemetry")]
    if let Some(provider) = TRACER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}
