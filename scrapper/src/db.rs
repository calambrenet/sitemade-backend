use chrono::{DateTime, Utc};
use futures_util::stream::StreamExt;
use mongodb::{
    options::{ClientOptions, ResolverConfig},
    Client,
};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseDomain {
    pub _id: mongodb::bson::oid::ObjectId,
    pub host: String,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
    pub scrappeable: bool,
    pub pagerank: Option<f64>,
    pub ip: Option<String>,
    pub languages: Option<Vec<String>>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseWebpage {
    pub _id: mongodb::bson::oid::ObjectId,
    pub domain_id: mongodb::bson::oid::ObjectId,
    pub url: String,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub scrapped_at: DateTime<Utc>,
    pub scrappeable: bool,
    pub technologies: Option<Vec<DatabaseWebTechnology>>,
    pub headers: Option<Vec<DatabaseWebTechnology>>,
    pub language: Option<String>,
    pub pagerank: Option<f64>,
}

impl DatabaseWebpage {
    pub fn new(domain_id: mongodb::bson::oid::ObjectId, url: String, pagerank: Option<f64>) -> Self {
        let birthay: DateTime<Utc> = chrono::DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
            .unwrap()
            .into();

        Self {
            _id: mongodb::bson::oid::ObjectId::new(),
            domain_id,
            url,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            scrapped_at: birthay,
            scrappeable: true,
            technologies: None,
            headers: None,
            language: None,
            pagerank: pagerank,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseWebTechnology {
    pub ttype: String,
    pub name: String,
}

pub async fn get_mongodb() -> Result<mongodb::Client, mongodb::error::Error> {
    let client_uri =
        env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment var!");

    let options =
        ClientOptions::parse_with_resolver_config(&client_uri, ResolverConfig::cloudflare())
            .await?;

    let db_client = Some(Client::with_options(options)?);

    return Ok(db_client.unwrap());
}

pub async fn get_database_domain(db_client: mongodb::Client, domain: &str) -> DatabaseDomain {
    let domains_collection: mongodb::Collection<DatabaseDomain> =
        db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "host": domain };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap_or_else(|_| None);

    let domain = match domain_result {
        Some(domain_doc) => domain_doc,
        None => {
            let domain_doc = DatabaseDomain {
                _id: mongodb::bson::oid::ObjectId::new(),
                host: domain.to_string(),
                created_at: chrono::Utc::now(),
                scrappeable: true,
                pagerank: None,
                ip: None,
                languages: None,
            };
            domains_collection
                .insert_one(&domain_doc, None)
                .await
                .unwrap();
            domain_doc
        }
    };

    domain
}

pub async fn update_database_domain_pagerank(db_client: mongodb::Client, domain_id: mongodb::bson::oid::ObjectId, pr: f64) {
    let domains_collection: mongodb::Collection<DatabaseDomain> =
        db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "_id": domain_id };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();
    match domain_result {
        Some(mut domain_doc) => {
            domain_doc.pagerank = Some(pr);
            let bson_doc = mongodb::bson::to_bson(&domain_doc).unwrap();
            domains_collection
                .update_one(
                    mongodb::bson::doc! { "_id": domain_id },
                    mongodb::bson::doc! { "$set": bson_doc },
                    None,
                )
                .await
                .unwrap();
        }
        None => {}
    };

    //actualizar el pagerank en database_webpages que sean de este dominio
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "domain_id": domain_id };

    let mut cursor = webpages_collection.find(webpage_doc, None).await.unwrap();

    while let Some(webpage) = cursor.next().await {
        match webpage {
            Ok(webpage_doc) => {
                //let mut webpage_doc: DatabaseWebpage =
                //    bson::from_document(webpage_doc).unwrap();
                let mut webpage_doc: DatabaseWebpage = webpage_doc;
                webpage_doc.pagerank = Some(pr);
                let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
                webpages_collection
                    .update_one(
                        mongodb::bson::doc! { "_id": webpage_doc._id },
                        mongodb::bson::doc! { "$set": bson_doc },
                        None,
                    )
                    .await
                    .unwrap();
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
}

pub async fn update_database_domain_ip(db_client: mongodb::Client, domain_id: mongodb::bson::oid::ObjectId, ip: String) {
    let domains_collection: mongodb::Collection<DatabaseDomain> =
        db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "_id": domain_id };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();
    match domain_result {
        Some(mut domain_doc) => {
            domain_doc.ip = Some(ip);
            let bson_doc = mongodb::bson::to_bson(&domain_doc).unwrap();
            domains_collection
                .update_one(
                    mongodb::bson::doc! { "_id": domain_id },
                    mongodb::bson::doc! { "$set": bson_doc },
                    None,
                )
                .await
                .unwrap();
        }
        None => {}
    };
}

pub async fn add_domain_to_database(db_client: mongodb::Client, domain: String) -> Option<mongodb::bson::oid::ObjectId> {
    let domains_collection: mongodb::Collection<DatabaseDomain> =
        db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "host": domain.clone() };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();

    let domain_id = match domain_result {
        Some(domain_doc) => domain_doc._id,
        None => {
            let domain_doc = DatabaseDomain {
                _id: mongodb::bson::oid::ObjectId::new(),
                host: domain.clone(),
                created_at: chrono::Utc::now(),
                scrappeable: true,
                pagerank: None,
                ip: None,
                languages: None,
            };
            domains_collection
                .insert_one(&domain_doc, None)
                .await
                .unwrap();
            domain_doc._id
        }
    };

    Some(domain_id)
}

pub async fn add_webpage_to_database(db_client: mongodb::Client, webpage: DatabaseWebpage) {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "url": webpage.url.clone() };

    let webpage_result = webpages_collection
        .find_one(webpage_doc, None)
        .await
        .unwrap();

    match webpage_result {
        Some(_webpage_doc) => {}
        None => {
            webpages_collection
                .insert_one(&webpage, None)
                .await
                .unwrap();
        }
    };
}

pub async fn set_database_webpage(
    db_client: mongodb::Client,
    webpage_url: String,
    domain_id: mongodb::bson::oid::ObjectId,
) -> DatabaseWebpage {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "url": webpage_url.clone() };

    let webpage_result = webpages_collection
        .find_one(webpage_doc, None)
        .await
        .unwrap();

    let webpage = match webpage_result {
        Some(webpage_doc) => webpage_doc,
        None => {
            let webpage_doc = DatabaseWebpage::new(domain_id, webpage_url.clone(), None);
            webpages_collection
                .insert_one(&webpage_doc, None)
                .await
                .unwrap();
            webpage_doc
        }
    };

    webpage
}

pub async fn update_database_web_technologies(
    db_client: mongodb::Client,
    dataabase_web_technologies: &Vec<DatabaseWebTechnology>,
    website_id: mongodb::bson::oid::ObjectId,
) -> Result<(), mongodb::error::Error> {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "_id": website_id };

    let webpage_result = webpages_collection
        .find_one(webpage_doc, None)
        .await
        .unwrap();

    match webpage_result {
        Some(mut webpage_doc) => {
            webpage_doc.technologies = Some(dataabase_web_technologies.clone());
            webpage_doc.updated_at = chrono::Utc::now();
            webpage_doc.scrapped_at = chrono::Utc::now();

            let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
            webpages_collection
                .update_one(
                    mongodb::bson::doc! { "_id": website_id },
                    mongodb::bson::doc! { "$set": bson_doc },
                    None,
                )
                .await
                .unwrap();
        }
        None => {}
    };

    return Ok(());
}

pub async fn update_database_web_headers(
    db_client: mongodb::Client,
    database_web_headers: &Vec<DatabaseWebTechnology>,
    website_id: mongodb::bson::oid::ObjectId,
) -> Result<(), mongodb::error::Error> {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "_id": website_id };

    let webpage_result = webpages_collection
        .find_one(webpage_doc, None)
        .await
        .unwrap();

    match webpage_result {
        Some(mut webpage_doc) => {
            webpage_doc.headers = Some(database_web_headers.clone());
            webpage_doc.updated_at = chrono::Utc::now();

            let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
            webpages_collection
                .update_one(
                    mongodb::bson::doc! { "_id": website_id },
                    mongodb::bson::doc! { "$set": bson_doc },
                    None,
                )
                .await
                .unwrap();
        }
        None => {}
    };

    return Ok(());
}

pub async fn update_database_webpage_language(
    db_client: mongodb::Client,
    language: String,
    domain_id: mongodb::bson::oid::ObjectId,
    website_id: mongodb::bson::oid::ObjectId,
) -> Result<(), mongodb::error::Error> {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "_id": website_id };

    let webpage_result = webpages_collection
        .find_one(webpage_doc, None)
        .await
        .unwrap();

    match webpage_result {
        Some(mut webpage_doc) => {
            webpage_doc.language = Some(language.clone());
            webpage_doc.updated_at = chrono::Utc::now();

            let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
            webpages_collection
                .update_one(
                    mongodb::bson::doc! { "_id": website_id },
                    mongodb::bson::doc! { "$set": bson_doc },
                    None,
                )
                .await
                .unwrap();
        }
        None => {}
    };

    let domains_collection: mongodb::Collection<DatabaseDomain> =
        db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "_id": domain_id };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();

    match domain_result {
        Some(mut domain_doc) => {
            match &mut domain_doc.languages {
                Some(languages) => {
                    if !languages.contains(&language) {
                        languages.push(language.clone());
                    }
                }
                None => {
                    domain_doc.languages = Some(vec![language.clone()]);
                }
            }
            let bson_doc = mongodb::bson::to_bson(&domain_doc).unwrap();
            domains_collection
                .update_one(
                    mongodb::bson::doc! { "_id": domain_id },
                    mongodb::bson::doc! { "$set": bson_doc },
                    None,
                )
                .await
                .unwrap();
        }
        None => {}
    };

    Ok(())
}

pub async fn update_database_webpages_set_scrappeable(
    db_client: mongodb::Client,
    website_id: mongodb::bson::oid::ObjectId,
    scrappeable: bool,
) -> Result<(), mongodb::error::Error> {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "_id": website_id };

    let mut webpage_result = webpages_collection
        .find(webpage_doc, None)
        .await
        .unwrap();

    while let Some(webpage) = webpage_result.next().await {
        match webpage {
            Ok(mut webpage_doc) => {
                webpage_doc.scrappeable = scrappeable;
                webpage_doc.updated_at = chrono::Utc::now();

                let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
                webpages_collection
                    .update_one(
                        mongodb::bson::doc! { "_id": website_id },
                        mongodb::bson::doc! { "$set": bson_doc },
                        None,
                    )
                    .await
                    .unwrap();
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    Ok(())
}

pub async fn get_database_webpage_to_scrap(db_client: mongodb::Client) -> Option<DatabaseWebpage> {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");

    let webpage_doc = mongodb::bson::doc! {
        "scrappeable": true,
        "scrapped_at": { "$lt": chrono::Utc::now() - chrono::Duration::days(10) }
    };

    //es necesario que scrapped_at al menos hace 10 díasS
    /*
    let webpage_doc = mongodb::bson::doc! {
        "scrappeable": true, "scrapped_at": { "$lt": chrono::Utc::now() - chrono::Duration::days(10) }
    };
    */
    //
    let pipeline = vec![
        mongodb::bson::doc! {
            "$match": webpage_doc
        },
        mongodb::bson::doc! {
            "$sort": {
                "scrapped_at": 1,
                //"pagerank": -1,
            }
        },
        mongodb::bson::doc! {
            "$limit": 1
        },
    ];

    let mut cursor = match webpages_collection.aggregate(pipeline, None).await {
        Ok(cursor) => cursor,
        Err(_) => {
            return None;
        }
    };

    //mostrar todos los resultados
    /*
     while let Some(result) = cursor.next().await {
        match result {
            Ok(document) => {
                println!("{:?}", document);
            },
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
    */

    let webpage = match cursor.next().await {
        Some(result) => {
            let doc: DatabaseWebpage = bson::from_document(result.unwrap()).unwrap();
            Some(doc)
        },
        None => {
            return None;
        }
    };

    return Some(webpage.unwrap());
}

pub async fn get_webpages_count_from_domain(db_client: mongodb::Client, domain_id: mongodb::bson::oid::ObjectId) -> u64 {
    let webpages_collection: mongodb::Collection<DatabaseWebpage> =
        db_client.database("sitemade").collection("webpages");

    let webpage_doc = mongodb::bson::doc! { "domain_id": domain_id };

    let count = webpages_collection.count_documents(webpage_doc, None).await.unwrap_or_else(|_| 0);

    count
}
