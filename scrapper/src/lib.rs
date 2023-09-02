mod db;

use cdns_rs::sync::request;
use regex::Regex;
use scraper::Html;
use serde::{Deserialize, Serialize};

use std::sync::Once;

use db::{DatabaseDomain, DatabaseWebTechnology, DatabaseWebpage};

#[macro_use]
extern crate log;

static INIT: Once = Once::new();

const TAGTYPESTRING: &str = "String";
const TAGTYPESTRINGREGEX: &str = "StringRegex";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tags {
    tag_type: String,
    tag_name: String,
    name: String,
    values: Vec<String>,
    parents: Vec<Tags>,
}

async fn get_page_language(
    db_client: mongodb::Client,
    document: &Html,
    database_domain: &DatabaseDomain,
    database_webpage: &DatabaseWebpage,
) {
    info!("     Buscando el idioma de la pagina");

    let mut active_lang: String = "".to_string();

    let meta_selector_list = [
        "meta[name=language]",
        "meta[property='og:locale']",
        "meta[http-equiv='Content-Language']",
    ];

    for meta_selector in meta_selector_list.iter() {
        let meta_language = scraper::Selector::parse(meta_selector).unwrap();
        let metas = document.select(&meta_language);
        for meta in metas {
            info!(
                "         meta({}) = {:?}",
                meta_selector,
                meta.value().attr("content").unwrap()
            );
            active_lang = meta.value().attr("content").unwrap().to_string();

            break;
        }
    }

    let html_selector = scraper::Selector::parse("html").unwrap();
    let html = document.select(&html_selector).next();
    if let Some(lang) = html {
        if let Some(lang_html) = lang.value().attr("lang") {
            info!("         html lang = {:?}", lang_html);

            if active_lang != lang_html.to_string() {
                active_lang = lang_html.to_string();
            }
        }
    }

    if active_lang != "" {
        /*
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(db::update_database_webpage_language(
                active_lang,
                database_domain._id,
                database_webpage._id,
            ))
            .unwrap();
            */
        db::update_database_webpage_language(
                db_client,
                active_lang,
                database_domain._id,
                database_webpage._id,
            ).await.unwrap();
    }
}

async fn get_pagerank(site_url: &str) -> f64 {
    let url = format!(
        "https://openpagerank.com/api/v1.0/getPageRank?domains[]={}",
        site_url
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.as_str())
        .header("API-OPR", "4cccg4w8ck0gg08csgcsc4o8c4wso4s4swk8oowo")
        .send().await;

    match response {
        Ok(response) => {
            let response_txt = response.text().await.unwrap();
            let response_json =
                serde_json::from_str::<serde_json::Value>(response_txt.as_str()).unwrap();
            if response_json["status_code"] == 200 {
                info!(
                    "     Pagerank = {:?}",
                    response_json["response"][0]["page_rank_decimal"]
                );
                info!("     Rank = {:?}", response_json["response"][0]["rank"]);

                response_json["response"][0]["page_rank_decimal"]
                    .as_f64()
                    .unwrap()
            } else {
                warn!(
                    "      Error al obtener el pagerank: {:?}",
                    response_json["status_msg"]
                );

                0.00
            }
        }
        Err(e) => {
            warn!("     Error al obtener el pagerank: {:?}", e);

            0.00
        }
    }
}

async fn get_country_region_from_ip(ip: Vec<std::net::IpAddr>) {
    let url = format!("https://ipapi.co/{}/json/", ip[0]);

    info!("     url = {}", url);
    info!("     Obtener el pais y region de la ip {}", ip[0]);

    let client = reqwest::Client::new();
    let response = client.get(url.as_str()).send().await;
    match response {
        Ok(response) => {
            let response_txt = response.text().await.unwrap();
            let response_json =
                serde_json::from_str::<serde_json::Value>(response_txt.as_str()).unwrap();

            if response_json["error"] != true {
                info!("         Country = {:?}", response_json["country_name"]);
                info!("          Region = {:?}", response_json["region"]);
            } else {
                warn!("         response_json = {:?}", response_json);
            }
        }
        Err(e) => {
            warn!("         Error al obtener el pais y region: {:?}", e);
        }
    }
}

pub struct Scrapper {
    site_url: String,
    site_domain: String,
}

impl Scrapper {
    /*
     * Obtiene las urls de sitios externos que puedan existir en el html
     *
     */
    async fn get_external_urls(&mut self, db_client: mongodb::Client, document: &Html, site_url: &str) {
        let a_selector = scraper::Selector::parse("a").unwrap();
        let a_list = document.select(&a_selector);
        for a in a_list {
            let href = match a.value().attr("href") {
                Some(href) => href,
                None => continue,
            };

            let site_url = site_url
                .replace("https://", "")
                .replace("http://", "")
                .replace("www.", "")
                .replace("/", "");

            let banned_list = [
                "facebook.com",
                "twitter.com",
                "instagram.com",
                "www.youtube.com",
                "linkedin.com",
                "pinterest.com",
                "tumblr.com",
                "reddit.com",
                "www.snapchat.com",
                "www.whatsapp.com",
                "www.messenger.com",
                "www.quora.com",
                "www.vk.com",
                "www.flickr.com",
                "www.meetup.com",
                "apple.com",
                "tiktok.com",
                "google.com",
                "spotify.com",
                "bit.ly",
            ];

            if href.contains(&site_url) || !href.starts_with("http") 
                || !href.starts_with("https") {
                continue;
            }
            if banned_list.iter().any(|banned| href.contains(banned)) {
                continue;
            }

            let re = Regex::new(r"(https?://)?(www\.)?([a-zA-Z0-9\-\.]+)").unwrap();
            let domain = re.captures(&href).unwrap().get(3).unwrap().as_str();

            let domain_id = db::add_domain_to_database(db_client.clone(), domain.to_string()).await;
            if domain_id != None {
                let database_domain = db::get_database_domain(db_client.clone(), domain).await;

                //Si hay menos de 2 webpages de este dominio lo añadimos a la base de datos
                //para que se pueda scrapear
                let webpages_count = db::get_webpages_count_from_domain(db_client.clone(), domain_id.unwrap()).await;
                if webpages_count < 2 { //FIXME: Usar constante
                    info!("     Enlace externo: {:?}", href);

                    let web_page = DatabaseWebpage::new(domain_id.unwrap(), href.to_string(), database_domain.pagerank);
                    db::add_webpage_to_database(db_client.clone(), web_page).await;
                } else {
                    //info!("     Ya hay {} paginas de este dominio", webpages_count);
                }
            }
        }
    }

    async fn search_tags_in_html(
        &mut self,
        db_client: mongodb::Client,
        response: String,
        tags_list: Vec<Tags>,
        _database_domain: &DatabaseDomain,
        database_webpage: &DatabaseWebpage,
    ) {
        info!("     Buscando tags en el html");

        let mut database_web_technologies = vec![];

        tags_list.iter().for_each(|tag| {
            //println!("Usar tag: {:?}", tag.name);

            if tag.tag_type == TAGTYPESTRING.to_string() {
                tag.values.iter().for_each(|value| {
                    if response.contains(value) {
                        info!(
                            "         Encontrado tecnología {:?} {:?}",
                            tag.tag_name, tag.name
                        );
                        info!("          Parents: {:?}", tag.parents);

                        if !database_web_technologies.iter().any(
                            |web_technology: &DatabaseWebTechnology| {
                                web_technology.name == tag.name
                            },
                        ) {
                            database_web_technologies.push(DatabaseWebTechnology {
                                ttype: tag.tag_name.clone(),
                                name: tag.name.clone(),
                            });
                        }
                    } else {
                        //println!("{}: not found", tag.name);
                    }
                })
            } else if tag.tag_type == TAGTYPESTRINGREGEX.to_string() {
                tag.values.iter().for_each(|value| {
                    let regex = Regex::new(value).unwrap();

                    //check if regex match
                    if regex.is_match(&response) {
                        info!(
                            "         Encontrado tecnología {:?} {:?}",
                            tag.tag_name, tag.name
                        );
                        info!("          Parents: {:?}", tag.parents);

                        if !database_web_technologies.iter().any(
                            |web_technology: &DatabaseWebTechnology| {
                                web_technology.name == tag.name
                            },
                        ) {
                            database_web_technologies.push(DatabaseWebTechnology {
                                ttype: tag.tag_name.clone(),
                                name: tag.name.clone(),
                            });
                        }
                    } else {
                        //println!("{}: ({:?}) not found", tag.name, value);
                    }
                });
            } else {
                warn!("     Tag type desconocido: {:?}", tag.tag_type);
            }
        });

        //info!("         database_web_technologies = {:?}", database_web_technologies);
        db::update_database_web_technologies(
                db_client.clone(),
                &database_web_technologies,
                database_webpage._id,
            ).await.unwrap();
    }

    async fn search_tags_in_headers(
        &mut self,
        db_client: mongodb::Client,
        headers: reqwest::header::HeaderMap,
        tags_list: Vec<Tags>,
        _database_domain: &DatabaseDomain,
        database_webpage: &DatabaseWebpage,
    ) {
        info!("     Buscando tags en los headers");
        let mut database_web_headers = vec![];

        tags_list.iter().for_each(|tag| {
            if tag.tag_type == TAGTYPESTRING.to_string() {
                tag.values.iter().for_each(|value| {
                    headers.iter().for_each(|(_header_key, header_value)| {
                        if header_value.to_str().unwrap().contains(value) {
                            info!(
                                "         Encontrado tecnología {:?} {:?}",
                                tag.tag_name, tag.name
                            );

                            if !database_web_headers.iter().any(
                                |web_technology: &DatabaseWebTechnology| {
                                    web_technology.name == tag.name
                                },
                            ) {
                                database_web_headers.push(DatabaseWebTechnology {
                                    ttype: tag.tag_name.clone(),
                                    name: tag.name.clone(),
                                });
                            }
                        } else {
                            //println!("{}: ({:?}) not found", tag.name, value);
                        }
                    });
                })
            } else if tag.tag_type == TAGTYPESTRINGREGEX.to_string() {
                tag.values.iter().for_each(|value| {
                    let regex = Regex::new(value).unwrap();

                    headers.iter().for_each(|(_key, value)| {
                        if regex.is_match(&value.to_str().unwrap()) {
                            info!(
                                "         Encontrado tecnología {:?} {:?}",
                                tag.tag_name, tag.name
                            );

                            if !database_web_headers.iter().any(
                                |web_technology: &DatabaseWebTechnology| {
                                    web_technology.name == tag.name
                                },
                            ) {
                                database_web_headers.push(DatabaseWebTechnology {
                                    ttype: tag.tag_name.clone(),
                                    name: tag.name.clone(),
                                });
                            }
                        } else {
                            //println!("{}: ({:?}) not found", tag.name, value);
                        }
                    });
                });
            } else {
                warn!("     Tag type desconocido: {:?}", tag.tag_type);
            }
        });

        db::update_database_web_headers(
                db_client.clone(),
                &database_web_headers,
                database_webpage._id,
            ).await.unwrap();
    }

    pub fn new() -> Self {
        Self {
            site_url: "".to_string(),
            site_domain: "".to_string(),
        }
    }

    fn init_logger(&mut self) {
        INIT.call_once(env_logger::init);
    }

    pub async fn scrap_all(&mut self) {
        self.init_logger();

        info!("Scrapping all...");
        let dbclient = db::get_mongodb().await.unwrap();

        loop {
            std::thread::sleep(std::time::Duration::from_secs(5));

            let database_webpage = db::get_database_webpage_to_scrap(dbclient.clone()).await;


            if database_webpage.is_none() {
                info!("No hay paginas para scrapear");

                return ();
            }

            let database_webpage = database_webpage.unwrap();

            //println!("database_webpage = {:?}", database_webpage);

            //si database_webpage.updated_at es menor a 10 dias ignorar
            /*
            let now = chrono::Utc::now();
            let duration = now.signed_duration_since(database_webpage.scrapped_at);
            let days = duration.num_days();
            if days < 10 {
                info!("La pagina {} fue scrappeada hace {} dias, ignorar", database_webpage.url, days);

                continue;
            }*/

            match self.scrap_site(database_webpage.url.clone(), Some(dbclient.clone())).await {
                Ok(_) => {
                    info!("Pagina {} scrapeada correctamente", database_webpage.url);
                }
                Err(e) => {
                    error!("Error al scrapear la pagina {}: {:?}", database_webpage.url, e);

                    db::update_database_webpages_set_scrappeable(
                        dbclient.clone(),
                        database_webpage._id,
                        false,
                    ).await.unwrap();
                }
            }
        }
    }

    pub async fn scrap_site(&mut self, site_url: String, dbclient: Option<mongodb::Client>) -> Result<(), reqwest::Error> {
        let db_client;
        if dbclient.is_none() {
            db_client = db::get_mongodb().await.unwrap();
        } else {
            db_client = dbclient.unwrap();
        }
        self.init_logger();

        info!("Scrapping... {}", site_url);

        let response        = reqwest::get(site_url.as_str()).await?;
        let headers         = response.headers().clone();
        let response_txt    = response.text().await?.clone();

        self.site_url = site_url.clone();

        let re      = Regex::new(r"(https?://)?(www\.)?([a-zA-Z0-9\-\.]+)").unwrap();
        let domain  = re.captures(&site_url).unwrap().get(3).unwrap().as_str();
        info!(" Domain = {}", domain);

        let database_domain = db::get_database_domain(db_client.clone(), domain).await;
        let database_webpage = db::set_database_webpage(
            db_client.clone(),
            site_url.clone(),
            database_domain._id
        ).await;

        info!(" domain_id = {:?}", database_domain._id);

        self.site_domain = domain.to_string();

        if database_domain.pagerank.is_none() {
            info!("Obtener el pagerank del sitio {}", domain);
            let pr = get_pagerank(domain).await;
            db::update_database_domain_pagerank(db_client.clone(), database_domain._id, pr).await;

            info!(" Pagerank = {:?}", pr);
        } else {
            let pr = database_domain.pagerank.unwrap();
            info!(" Pagerank = {:?}", pr)
        }

        let res_a = request::resolve_fqdn(domain, None);
        let ip = match res_a {
            Ok(ip) => ip,
            Err(e) => {
                error!("Error al obtener la ip: {:?}", e);

                return Ok(());
            }
        };

        info!(" IP = {:?}", ip);
        if ip.len() > 0 {
            get_country_region_from_ip(ip.clone()).await;

            db::update_database_domain_ip(
                    db_client.clone(),
                    database_domain._id,
                    ip[0].to_string(),
            ).await;
        }

        //Listado de Tags
        let file = std::fs::File::open("body_tags.yaml").unwrap();
        let body_tags_list: Vec<Tags> = serde_yaml::from_reader(file).unwrap();

        let file = std::fs::File::open("headers_tags.yaml").unwrap();
        let headers_tags_list: Vec<Tags> = serde_yaml::from_reader(file).unwrap();

        
        //println!("headers = {:?}", headers);
        self.search_tags_in_html(
            db_client.clone(),
            response_txt.clone(),
            body_tags_list.clone(),
            &database_domain,
            &database_webpage,
        ).await;
        self.search_tags_in_headers(
            db_client.clone(),
            headers,
            headers_tags_list.clone(),
            &database_domain,
            &database_webpage,
        ).await;

        //Ahora analizar el html
        let document = scraper::Html::parse_document(&response_txt);

        //buscamos el idioma de la pagina
        get_page_language(db_client.clone(), &document, &database_domain, &database_webpage).await;

        //Obtener urls de sitios externos
        self.get_external_urls(db_client.clone(), &document, domain).await;

        info!("Scraping finished!");
        
        Ok(())
    }
}
