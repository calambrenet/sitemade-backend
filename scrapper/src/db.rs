use std::env;
use mongodb::{Client, options::{ClientOptions, ResolverConfig}};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

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
    pub updated_dat: DateTime<Utc>,
    pub scrappeable: bool,
    pub technologies: Option<Vec<DatabaseWebTechnology>>,
    pub headers: Option<Vec<DatabaseWebTechnology>>,
    pub language: Option<String>,
}

impl DatabaseWebpage {
    pub fn new(domain_id: mongodb::bson::oid::ObjectId, url: String) -> Self {
        Self {
            _id: mongodb::bson::oid::ObjectId::new(),
            domain_id,
            url,
            created_at: chrono::Utc::now(),
            updated_dat: chrono::Utc::now(),
            scrappeable: true,
            technologies: None,
            headers: None,
            language: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseWebTechnology {
    pub ttype: String,
    pub name: String,
}

async fn get_mongodb() -> Result<mongodb::Client, mongodb::error::Error> {
    let client_uri =
        env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment var!");

    let options =
        ClientOptions::parse_with_resolver_config(&client_uri, ResolverConfig::cloudflare())
        .await?;

    let db_client = Some(Client::with_options(options)?);

    return Ok(db_client.unwrap());
}

pub async fn get_database_domain(domain: &str) -> DatabaseDomain {
    let db_client = get_mongodb().await.unwrap();

    let domains_collection:mongodb::Collection<DatabaseDomain> = db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "host": domain };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();
    let domain = match domain_result {
        Some(domain_doc) => {
            domain_doc
        },
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
            domains_collection.insert_one(&domain_doc, None).await.unwrap();
            domain_doc
        }
    };

    domain
}

pub async fn update_database_domain_pagerank(domain_id: mongodb::bson::oid::ObjectId, pr: f64) {
    let db_client = get_mongodb().await.unwrap();

    let domains_collection:mongodb::Collection<DatabaseDomain> = db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "_id": domain_id };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();
    match domain_result {
        Some(mut domain_doc) => {
            domain_doc.pagerank = Some(pr);
            let bson_doc = mongodb::bson::to_bson(&domain_doc).unwrap();
            domains_collection.update_one(mongodb::bson::doc! { "_id": domain_id },
                                          mongodb::bson::doc! { "$set": bson_doc }, None)
                .await.unwrap();
        },
        None => {
        }
    };
}

pub async fn update_database_domain_ip(domain_id: mongodb::bson::oid::ObjectId, ip: String) {
    let db_client = get_mongodb().await.unwrap();

    let domains_collection:mongodb::Collection<DatabaseDomain> = db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "_id": domain_id };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();
    match domain_result {
        Some(mut domain_doc) => {
            domain_doc.ip = Some(ip);
            let bson_doc = mongodb::bson::to_bson(&domain_doc).unwrap();
            domains_collection.update_one(mongodb::bson::doc! { "_id": domain_id },
                                          mongodb::bson::doc! { "$set": bson_doc }, None)
                .await.unwrap();
        },
        None => {
        }
    };
}

pub async fn add_domain_to_database(domain: String) -> mongodb::bson::oid::ObjectId {
    let db_client = get_mongodb().await.unwrap();

    let domains_collection:mongodb::Collection<DatabaseDomain> = db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "host": domain.clone() };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();

    let domain_id = match domain_result {
        Some(domain_doc) => {
            domain_doc._id
        },
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
            domains_collection.insert_one(&domain_doc, None).await.unwrap();
            domain_doc._id
        }
    };

    domain_id
}

pub async fn add_webpage_to_database(webpage: DatabaseWebpage) {
    let db_client = get_mongodb().await.unwrap();

    let webpages_collection:mongodb::Collection<DatabaseWebpage> = db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "url": webpage.url.clone() };

    let webpage_result = webpages_collection.find_one(webpage_doc, None).await.unwrap();

    match webpage_result {
        Some(_webpage_doc) => {
        },
        None => {
            webpages_collection.insert_one(&webpage, None).await.unwrap();
        }
    };
}

pub async fn set_database_webpage(webpage_url: String, domain_id: mongodb::bson::oid::ObjectId) -> DatabaseWebpage {
    let db_client = get_mongodb().await.unwrap();

    let webpages_collection:mongodb::Collection<DatabaseWebpage> = db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "url": webpage_url.clone() };

    let webpage_result = webpages_collection.find_one(webpage_doc, None).await.unwrap();

    let webpage = match webpage_result {
        Some(webpage_doc) => {
            webpage_doc
        },
        None => {
            let webpage_doc = DatabaseWebpage::new(domain_id, webpage_url.clone());
            webpages_collection.insert_one(&webpage_doc, None).await.unwrap();
            webpage_doc
        }
    };

    webpage
}

pub async fn update_database_web_technologies(
                                          dataabase_web_technologies: &Vec<DatabaseWebTechnology>,
                                          website_id: mongodb::bson::oid::ObjectId) -> Result<(), mongodb::error::Error> {

    let db_client = get_mongodb().await.unwrap();

    let webpages_collection:mongodb::Collection<DatabaseWebpage> = db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "_id": website_id };

    let webpage_result = webpages_collection.find_one(webpage_doc, None).await.unwrap();

    match webpage_result {
        Some(mut webpage_doc) => {
            webpage_doc.technologies = Some(dataabase_web_technologies.clone());
            webpage_doc.updated_dat = chrono::Utc::now();

            let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
            webpages_collection.update_one(mongodb::bson::doc! { "_id": website_id },
                                           mongodb::bson::doc! { "$set": bson_doc }, None)
                .await.unwrap();
        },
        None => {
        }
    };

    return Ok(());
}

pub async fn update_database_web_headers(
                                     database_web_headers: &Vec<DatabaseWebTechnology>,
                                     website_id: mongodb::bson::oid::ObjectId) -> Result<(), mongodb::error::Error> {

    let db_client = get_mongodb().await.unwrap();

    let webpages_collection:mongodb::Collection<DatabaseWebpage> = db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "_id": website_id };

    let webpage_result = webpages_collection.find_one(webpage_doc, None).await.unwrap();

    match webpage_result {
        Some(mut webpage_doc) => {
            webpage_doc.headers = Some(database_web_headers.clone());
            webpage_doc.updated_dat = chrono::Utc::now();

            let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
            webpages_collection.update_one(mongodb::bson::doc! { "_id": website_id },
                                           mongodb::bson::doc! { "$set": bson_doc }, None)
                .await.unwrap();
        },
        None => {
        }
    };

    return Ok(());
}

pub async fn update_database_webpage_language(
    language: String,
    domain_id: mongodb::bson::oid::ObjectId,
    website_id: mongodb::bson::oid::ObjectId) -> Result<(), mongodb::error::Error> {

    let db_client = get_mongodb().await.unwrap();

    let webpages_collection:mongodb::Collection<DatabaseWebpage> = db_client.database("sitemade").collection("webpages");
    let webpage_doc = mongodb::bson::doc! { "_id": website_id };

    let webpage_result = webpages_collection.find_one(webpage_doc, None).await.unwrap();

    match webpage_result {
        Some(mut webpage_doc) => {
            webpage_doc.language = Some(language.clone());
            webpage_doc.updated_dat = chrono::Utc::now();

            let bson_doc = mongodb::bson::to_bson(&webpage_doc).unwrap();
            webpages_collection.update_one(mongodb::bson::doc! { "_id": website_id },
                                           mongodb::bson::doc! { "$set": bson_doc }, None)
                .await.unwrap();
        },
        None => {
        }
    };

    let domains_collection:mongodb::Collection<DatabaseDomain> = db_client.database("sitemade").collection("domains");
    let domain_doc = mongodb::bson::doc! { "_id": domain_id };

    let domain_result = domains_collection.find_one(domain_doc, None).await.unwrap();

    match domain_result {
        Some(mut domain_doc) => {
            match &mut domain_doc.languages {
                Some(languages) => {
                    if !languages.contains(&language) {
                        languages.push(language.clone());
                    }
                },
                None => {
                    domain_doc.languages = Some(vec![language.clone()]);
                }
            }
            let bson_doc = mongodb::bson::to_bson(&domain_doc).unwrap();
            domains_collection.update_one(mongodb::bson::doc! { "_id": domain_id },
                                          mongodb::bson::doc! { "$set": bson_doc }, None)
                .await.unwrap();
        },
        None => {
        }
    };

    Ok(())
}

