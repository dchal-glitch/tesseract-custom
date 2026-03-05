use serde_derive::{Serialize, Deserialize};

use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use std::thread;

use actix_web::{
    HttpRequest,
    HttpResponse,
    Result as ActixResult,
};

use crate::app::{AppState, SchemaSource};
use crate::logic_layer;
use crate::schema_config;


#[derive(Debug, Deserialize, Serialize)]
pub struct FlushQueryOpt {
    pub secret: String,
}

pub fn flush_handler(req: HttpRequest<AppState>) -> ActixResult<HttpResponse> {
    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query_res = QS_NON_STRICT.deserialize_str::<FlushQueryOpt>(&query);
    let query = match query_res {
        Ok(q) => q,
        Err(err) => {
            return Ok(HttpResponse::BadRequest().json(err.to_string()));
        },
    };

    let db_secret = match &req.state().env_vars.flush_secret {
        Some(db_secret) => db_secret,
        None => { return Ok(HttpResponse::Unauthorized().finish()); }
    };

    if query.secret == *db_secret {
        info!("Flush internal state");

        // Read schema again
        // NOTE: This logic will change once we start supporting remote schemas
        let schema_path = match &req.state().env_vars.schema_source {
            SchemaSource::LocalSchema { ref filepath } => filepath.clone(),
            SchemaSource::RemoteSchema { ref endpoint } => endpoint.clone(),
        };

        let schema = match schema_config::read_schema(&schema_path) {
            Ok(val) => val,
            Err(err) => {
                error!("{}", err);
                return Ok(HttpResponse::InternalServerError().finish());
            },
        };

        // Update shared schema
        {
            let mut w = req.state().schema.write().unwrap();
            *w = schema.clone();
        }

        // Re-populate cache in a separate thread with its own actix System.
        // We cannot block_on from within the request handler (same runtime = deadlock),
        // so we spawn a thread that creates a fresh System for cache population.
        let backend = req.state().backend.clone();
        let logic_layer_config = req.state().logic_layer_config.as_ref()
            .and_then(|arc| arc.read().ok())
            .map(|guard| (*guard).clone());
        let cache_arc = req.state().cache.clone();

        let handle = thread::spawn(move || {
            let mut sys = actix::System::new("flush-cache");
            match logic_layer::populate_cache(
                schema,
                &logic_layer_config,
                backend,
                &mut sys,
            ) {
                Ok(cache) => {
                    if let Ok(mut w) = cache_arc.write() {
                        *w = cache;
                        info!("Cache repopulated successfully");
                    }
                }
                Err(err) => {
                    error!("Cache repopulation failed: {}", err);
                }
            }
        });

        if let Err(e) = handle.join() {
            error!("Flush thread panicked: {:?}", e);
            return Ok(HttpResponse::InternalServerError().finish());
        }

        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::Unauthorized().finish())
    }
}
