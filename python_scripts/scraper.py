from scrapfly import ScrapflyClient, ScrapeConfig
from scrapfly.errors import ScrapflyAspError
import os
from dotenv import load_dotenv
import csv
import re
import json
from urllib.parse import urlencode
import asyncio
from typing import List
import random

load_dotenv()
scrapfly_key = os.environ["SCRAPFLY_KEY"]
scrapfly = ScrapflyClient(key=scrapfly_key)
os.makedirs("data", exist_ok=True)

def parse_search_page(html: str):
    matches = re.findall(r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});', html)
    if not matches:
        raise ValueError("Could not find job data in HTML")
    data = json.loads(matches[0])
    return {
        "results": data["metaData"]["mosaicProviderJobCardsModel"]["results"],
        "meta": data["metaData"]["mosaicProviderJobCardsModel"]["tierSummaries"],
    }
    
    
def parse_job_page(html: str):
    matches = re.findall(r"_initialData=(\{.+?\});", html)
    if not matches:
        return {}
    data = json.loads(matches[0])
    return data.get("jobInfoWrapperModel", {}).get("jobInfoModel", {})


async def scrape_with_retry(scrape_config, retries=3, delay=2):
    """Retry scraper with exponential backoff on ASP failure"""
    for attempt in range(1, retries + 1):
        try:
            result = await scrapfly.async_scrape(scrape_config)
            return result
        except ScrapflyAspError as e:
            print(f"Attempt {attempt} failed: {e}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= 2  # exponential backoff
    raise RuntimeError(f"Failed after {retries} attempts: {scrape_config.url}")


async def scrape_search(query: str, location: str, max_results: int = 50):
    """Scrape job listings for a search query"""
    def make_page_url(offset):
        params = {"q": query, "l": location, "filter": 0, "start": offset}
        return "https://www.indeed.com/jobs?" + urlencode(params)

    print(f"Scraping first page: query={query}, location={location}")
    first_result = await scrape_with_retry(ScrapeConfig(make_page_url(0), asp=True, render_js=True, country="us", session="indeed-session",))
    data_first_page = parse_search_page(first_result.content)

    results = data_first_page["results"]
    total_results = sum(cat["jobCount"] for cat in data_first_page["meta"])
    total_results = min(total_results, max_results)

    # Compute offsets for remaining pages
    remaining_pages = (total_results - 10 + 9) // 10  # ceil division
    offsets = [10 * i for i in range(1, remaining_pages + 1)]

    for offset in offsets:
        print(f"Scraping page with offset {offset}")
        page_result = await scrape_with_retry(ScrapeConfig(make_page_url(offset), asp=True, render_js=True, country="us", session="indeed-session",))
        page_data = parse_search_page(page_result.content)
        results.extend(page_data["results"])
        await asyncio.sleep(random.uniform(1, 3))  # small random delay to avoid detection

    return results[:max_results]


async def scrape_jobs(job_keys: List[str]):
    """Scrape job details for a list of job keys"""
    detailed_jobs = []
    for key in job_keys:
        url = f"https://www.indeed.com/m/basecamp/viewjob?viewtype=embedded&jk={key}"
        print(f"Scraping job: {key}")
        result = await scrape_with_retry(ScrapeConfig(url, asp=True, render_js=True, country="us", session="indeed-session",))
        job_data = parse_job_page(result.content)
        if job_data:
            detailed_jobs.append(job_data)
        await asyncio.sleep(random.uniform(1, 2))
    return detailed_jobs


def save_jobs_to_csv(jobs, filename="data/jobs.csv"):
    if not jobs:
        print("No jobs to save.")
        return
    keys = sorted(jobs[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for job in jobs:
            writer.writerow(job)
    print(f"Saved {len(jobs)} jobs to {filename}")



async def main():
    search_results = await scrape_search(query="software", location="Stockholm", max_results=50)
    job_keys = [job["jobKey"] for job in search_results]
    detailed_jobs = await scrape_jobs(job_keys)
    save_jobs_to_csv(detailed_jobs)

if __name__ == "__main__":
    asyncio.run(main())