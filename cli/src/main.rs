use dotenv::dotenv;
use std::env;


fn main() {
    println!("SiteMade CLI");
    dotenv().ok();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Processing sites");

        let mut scrapper = scrapper::Scrapper::new();
        tokio::runtime::Runtime::new().unwrap().block_on(scrapper.scrap_all());
    } else {
        let site = args[1].clone();
        if !site.to_lowercase().starts_with("http") && !site.to_lowercase().starts_with("https") {
            println!("The site must start with http or https");
            return;
        }

        let mut scrapper = scrapper::Scrapper::new();
        //scrapper.scrap_site(site).await;
        tokio::runtime::Runtime::new().unwrap().block_on(scrapper.scrap_site(site, None)).unwrap();
    }
}
