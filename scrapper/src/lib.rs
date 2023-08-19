mod db;

use regex::Regex;
use scraper::Html;
use cdns_rs::sync::request;
use serde::{Serialize, Deserialize};

use db::{DatabaseDomain, DatabaseWebpage, DatabaseWebTechnology};

#[macro_use]
extern crate log;


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

fn get_page_language(document: &Html,
                     database_domain: &DatabaseDomain,
                     database_webpage: &DatabaseWebpage) {

    info!("     Buscando el idioma de la pagina");

    let mut active_lang: String  = "".to_string();

    let meta_selector_list = [
        "meta[name=language]",
        "meta[property='og:locale']",
        "meta[http-equiv='Content-Language']"
    ];

    for meta_selector in meta_selector_list.iter() {
        let meta_language = scraper::Selector::parse(meta_selector).unwrap();
        let metas = document.select(&meta_language);
        for meta in metas {
            info!("         meta({}) = {:?}", meta_selector, meta.value().attr("content").unwrap());
            active_lang = meta.value().attr("content").unwrap().to_string();

            break;
        }
    }

    let html_selector = scraper::Selector::parse("html").unwrap();
    let html = document.select(&html_selector).next();
    if let Some(lang) = html {
        info!("         html lang = {:?}", lang.value().attr("lang").unwrap());

        if active_lang != lang.value().attr("lang").unwrap().to_string() {
            active_lang = lang.value().attr("lang").unwrap().to_string();
        }
    }

    if active_lang != "" {
        tokio::runtime::Runtime::new().unwrap().block_on(db::update_database_webpage_language(active_lang, database_domain._id, database_webpage._id)).unwrap();
    }
}


fn get_pagerank(site_url: &str) -> f64 {
    let url = format!("https://openpagerank.com/api/v1.0/getPageRank?domains[]={}", site_url);
    let client = reqwest::blocking::Client::new();
    let response = client.get(url.as_str())
        .header("API-OPR", "4cccg4w8ck0gg08csgcsc4o8c4wso4s4swk8oowo")
        .send();

    match response {
        Ok(response) => {
            let response_txt = response.text().unwrap();
            let response_json = serde_json::from_str::<serde_json::Value>(response_txt.as_str()).unwrap();
            if response_json["status_code"] == 200 {
                info!("     Pagerank = {:?}", response_json["response"][0]["page_rank_decimal"]);
                info!("     Rank = {:?}", response_json["response"][0]["rank"]);


                response_json["response"][0]["page_rank_decimal"].as_f64().unwrap()
            } else {
                warn!("      Error al obtener el pagerank: {:?}", response_json["status_msg"]);

                0.00
            }
        },
        Err(e) => {
            warn!("     Error al obtener el pagerank: {:?}", e);

            0.00
        }
    }
}

fn get_country_region_from_ip(ip: Vec<std::net::IpAddr>) {
    let url = format!("https://ipapi.co/{}/json/", ip[0]);

    info!("     url = {}", url);
    info!("     Obtener el pais y region de la ip {}", ip[0]);

    let client = reqwest::blocking::Client::new();
    let response = client.get(url.as_str()).send();
    match response {
        Ok(response) => {
            let response_txt = response.text().unwrap();
            let response_json = serde_json::from_str::<serde_json::Value>(response_txt.as_str()).unwrap();

            if response_json["error"] != true {
                info!("         Country = {:?}", response_json["country_name"]);
                info!("          Region = {:?}", response_json["region"]);
            } else {
                warn!("         response_json = {:?}", response_json);
            }
        },
        Err(e) => {
            warn!("         Error al obtener el pais y region: {:?}", e);
        }
    }
}

pub struct Scrapper {
    site_url: String,
    site_domain: String,
}


impl Scrapper{

    /*
     * Obtiene las urls de sitios externos que puedan existir en el html
     *
     */
    fn get_external_urls(&mut self, document: &Html, site_url: &str) {
        let a_selector = scraper::Selector::parse("a").unwrap();
        let a_list = document.select(&a_selector);
        for a in a_list {
            //si el enlace no es del mismo dominio lo guardamos en un listado
            let href = match a.value().attr("href") {
                Some(href) => href,
                None => continue,
            };

            let site_url = site_url.replace("https://", "").replace("http://", "").replace("www.", "").replace("/", "");

            //si el enlace incluye el valor de site_url se ignora
            //debe ser un enlace valido que empiece por http o https
            if href.contains(&site_url) || !href.starts_with("http") || !href.starts_with("https") {
                continue;
            }

            info!("     Enlace externo: {:?}", href);

            let re = Regex::new(r"(https?://)?(www\.)?([a-zA-Z0-9\-\.]+)").unwrap();
            let domain = re.captures(&href).unwrap().get(3).unwrap().as_str();

            //Añadir dominio a la base de datos
            let domain_id = tokio::runtime::Runtime::new().unwrap().block_on(db::add_domain_to_database(domain.to_string()));
            let web_page = DatabaseWebpage::new(domain_id, href.to_string());
            tokio::runtime::Runtime::new().unwrap().block_on(db::add_webpage_to_database(web_page));
        }
    }


    fn search_tags_in_html(&mut self, response: String, tags_list: Vec<Tags>, _database_domain: &DatabaseDomain, database_webpage: &DatabaseWebpage) {
        info!("     Buscando tags en el html");

        let mut database_web_technologies = vec![];

        tags_list.iter().for_each(|tag| {
            //println!("Usar tag: {:?}", tag.name);

            if tag.tag_type == TAGTYPESTRING.to_string() {
                tag.values.iter().for_each(|value| {
                    if response.contains(value) {
                        info!("         Encontrado tecnología {:?} {:?}", tag.tag_name, tag.name);
                        info!("          Parents: {:?}", tag.parents);

                        if !database_web_technologies.iter().any(|web_technology: &DatabaseWebTechnology| web_technology.name == tag.name) {
                            database_web_technologies.push(
                                DatabaseWebTechnology {
                                    ttype: tag.tag_name.clone(),
                                    name: tag.name.clone(),
                                }
                            );
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
                        info!("         Encontrado tecnología {:?} {:?}", tag.tag_name, tag.name);
                        info!("          Parents: {:?}", tag.parents);

                        if !database_web_technologies.iter().any(|web_technology: &DatabaseWebTechnology| web_technology.name == tag.name) {
                            database_web_technologies.push(
                                DatabaseWebTechnology {
                                    ttype: tag.tag_name.clone(),
                                    name: tag.name.clone(),
                                }
                            );
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

        tokio::runtime::Runtime::new().unwrap().block_on(db::update_database_web_technologies(&database_web_technologies, database_webpage._id)).unwrap();
    }


    fn search_tags_in_headers(&mut self, headers: reqwest::header::HeaderMap, tags_list: Vec<Tags>, _database_domain: &DatabaseDomain, database_webpage: &DatabaseWebpage) {
        info!("     Buscando tags en los headers");
        let mut database_web_headers = vec![];

        tags_list.iter().for_each(|tag| {
            if tag.tag_type == TAGTYPESTRING.to_string() {
                tag.values.iter().for_each(|value| {
                    headers.iter().for_each(|(_header_key, header_value)| {
                        if header_value.to_str().unwrap().contains(value) {
                            info!("         Encontrado tecnología {:?} {:?}", tag.tag_name, tag.name);

                            if !database_web_headers.iter().any(|web_technology: &DatabaseWebTechnology| web_technology.name == tag.name) {
                                database_web_headers.push(
                                    DatabaseWebTechnology {
                                        ttype: tag.tag_name.clone(),
                                        name: tag.name.clone(),
                                    }
                                );
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
                            info!("         Encontrado tecnología {:?} {:?}", tag.tag_name, tag.name);

                            if !database_web_headers.iter().any(|web_technology: &DatabaseWebTechnology| web_technology.name == tag.name) {
                                database_web_headers.push(
                                    DatabaseWebTechnology {
                                        ttype: tag.tag_name.clone(),
                                        name: tag.name.clone(),
                                    }
                                );
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

        tokio::runtime::Runtime::new().unwrap().block_on(db::update_database_web_headers(&database_web_headers, database_webpage._id)).unwrap();
    }

    pub fn new() -> Self {
        Self {
            site_url: "".to_string(),
            site_domain: "".to_string(),
        }
    }

    pub fn scrap_site(&mut self, site_url: String) {
        env_logger::init();

        info!("Scrapping... {}", site_url);

        let response = match reqwest::blocking::get(site_url.as_str()) {
            Ok(response) => response,
            Err(e) => {
                error!("Error al obtener la pagina: {:?}", e);
                
                return;
            }
        };

        self.site_url = site_url.clone();

        let re = Regex::new(r"(https?://)?(www\.)?([a-zA-Z0-9\-\.]+)").unwrap();
        let domain = re.captures(&site_url).unwrap().get(3).unwrap().as_str();
        info!(" Domain = {}", domain);

        let database_domain = tokio::runtime::Runtime::new().unwrap().block_on(db::get_database_domain(domain));
        let database_webpage = tokio::runtime::Runtime::new().unwrap().block_on(db::set_database_webpage(site_url.clone(), database_domain._id));

        info!(" domain_id = {:?}", database_domain._id);

        self.site_domain = domain.to_string();

        if database_domain.pagerank.is_none() {
            info!("Obtener el pagerank del sitio {}", domain);
            let pr = get_pagerank(domain);

            tokio::runtime::Runtime::new().unwrap().block_on(db::update_database_domain_pagerank(database_domain._id, pr));

            info!(" Pagerank = {:?}", pr);
        } else {
            let pr = database_domain.pagerank.unwrap();
            info!(" Pagerank = {:?}", pr);
        }

        let res_a = request::resolve_fqdn(domain, None);
        let ip = match res_a {
            Ok(res_a) => res_a,
            Err(e) => {
                error!("Error al obtener la ip del sitio: {:?}", e);
                
                return;
            }
        };

        info!(" IP = {:?}", ip);
        get_country_region_from_ip(ip.clone());

        tokio::runtime::Runtime::new().unwrap().block_on(db::update_database_domain_ip(database_domain._id, ip[0].to_string()));

        //Listado de Tags
        let file = std::fs::File::open("body_tags.yaml").unwrap();
        let body_tags_list: Vec<Tags> = serde_yaml::from_reader(file).unwrap();

        let file = std::fs::File::open("headers_tags.yaml").unwrap();
        let headers_tags_list: Vec<Tags> = serde_yaml::from_reader(file).unwrap();

        let headers = response.headers().clone();
        let response_txt = response.text().unwrap();

        //println!("headers = {:?}", headers);
        self.search_tags_in_html(response_txt.clone(), body_tags_list.clone(), &database_domain, &database_webpage);
        self.search_tags_in_headers(headers, headers_tags_list.clone(), &database_domain, &database_webpage);

        //Ahora analizar el html
        let document = scraper::Html::parse_document(&response_txt);

        //buscamos el idioma de la pagina
        get_page_language(&document, &database_domain, &database_webpage);

        //Obtener urls de sitios externos
        self.get_external_urls(&document, domain);

        info!("Scraping finished!");
    }
}

