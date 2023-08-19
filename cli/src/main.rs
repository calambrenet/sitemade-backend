use scrapper;
use std::env;
use dotenv::dotenv;

fn main() {
    println!("SiteMade CLI");
    dotenv().ok();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Please provide a site to scrap");
        return;
    }

    let site = args[1].clone();
    if !site.to_lowercase().starts_with("http")
        && !site.to_lowercase().starts_with("https") {
        println!("The site must start with http or https");
        return;
    }

    let mut scrapper = scrapper::Scrapper::new();
    scrapper.scrap_site(site);
}
