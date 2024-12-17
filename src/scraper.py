# File: projectw/src/scraper.py

import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import logging
import os
import csv
from typing import Set, Dict, List, Optional, Tuple
import ssl
import certifi
from aiohttp import ClientTimeout, TCPConnector, ClientSession
import random
import time
from datetime import datetime
import re
from urllib.parse import urlparse, urljoin
from tqdm import tqdm
import signal
import sys

class WebScraper:
    def __init__(
        self,
        input_file: str,
        output_dir: str = 'output',
        batch_size: int = 50,
        max_retries: int = 3
    ):
        # Configuration
        self.input_file = input_file
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        # Create output directories
        os.makedirs(output_dir, exist_ok=True)
        
        # Setup files
        self.whatsapp_links_file = f"{output_dir}/whatsapp_links.csv"
        self.success_file = f"{output_dir}/success_report.csv"
        self.error_file = f"{output_dir}/error_report.csv"
        self.progress_file = f"{output_dir}/progress.csv"
        
        # Initialize files with headers
        self._init_files()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(f"{output_dir}/scraper.log"),
                logging.StreamHandler()
            ]
        )
        
        # Stats tracking
        self.stats = {
            'total_urls': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'whatsapp_links': 0,
            'start_time': time.time()
        }
        
        # Load progress if exists
        self.processed_urls = self._load_progress()
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _init_files(self):
        """Initialize output files with headers"""
        if not os.path.exists(self.whatsapp_links_file):
            with open(self.whatsapp_links_file, 'w', newline='') as f:
                csv.writer(f).writerow(['source_url', 'whatsapp_link'])
                
        if not os.path.exists(self.success_file):
            with open(self.success_file, 'w', newline='') as f:
                csv.writer(f).writerow(['url', 'processing_time', 'links_found'])
                
        if not os.path.exists(self.error_file):
            with open(self.error_file, 'w', newline='') as f:
                csv.writer(f).writerow(['url', 'error_type', 'error_message', 'attempts'])

    def _load_progress(self) -> Set[str]:
        """Load previously processed URLs"""
        processed = set()
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                processed = set(line.strip() for line in f)
            logging.info(f"Loaded {len(processed)} previously processed URLs")
        return processed

    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        logging.info("\nShutdown signal received. Saving progress...")
        self._save_progress()
        sys.exit(0)

    def _save_progress(self):
        """Save progress to file"""
        with open(self.progress_file, 'w') as f:
            for url in self.processed_urls:
                f.write(f"{url}\n")

    async def create_session(self) -> ClientSession:
        """Create optimized aiohttp session"""
        # SSL context
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Connection pooling
        connector = TCPConnector(
            ssl=ssl_context,
            limit_per_host=10,
            ttl_dns_cache=300,
            force_close=False
        )
        
        return ClientSession(
            connector=connector,
            timeout=ClientTimeout(total=30),
            headers=self._get_headers()
        )

    def _get_headers(self) -> Dict[str, str]:
        """Get random user agent headers"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) Firefox/120.0'
        ]
        
        return {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    def extract_whatsapp_links(self, html_content: str) -> Set[str]:
        """Extract WhatsApp links from HTML content"""
        whatsapp_patterns = [
            r'https://chat\.whatsapp\.com/[A-Za-z0-9]+',
            r'https://wa\.me/[0-9]+',
            r'whatsapp\.com/send\?phone=[0-9]+'
        ]
        
        links = set()
        for pattern in whatsapp_patterns:
            found = re.findall(pattern, html_content, re.IGNORECASE)
            links.update(found)
        
        return links

    async def process_url(
        self,
        session: ClientSession,
        url: str
    ) -> Tuple[bool, Set[str], Optional[str]]:
        """Process a single URL with retries"""
        start_time = time.time()
        
        # Format URL if needed
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
        
        for attempt in range(self.max_retries):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        links = self.extract_whatsapp_links(content)
                        
                        # Save successful result
                        processing_time = time.time() - start_time
                        with open(self.success_file, 'a', newline='') as f:
                            csv.writer(f).writerow([
                                url, f"{processing_time:.2f}s", len(links)
                            ])
                        
                        # Save found links
                        if links:
                            with open(self.whatsapp_links_file, 'a', newline='') as f:
                                writer = csv.writer(f)
                                for link in links:
                                    writer.writerow([url, link])
                        
                        return True, links, None
                    
                    elif response.status == 404:
                        return False, set(), f"Not found (404)"
                    elif response.status == 403:
                        return False, set(), f"Forbidden (403)"
                    else:
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return False, set(), f"HTTP {response.status}"
                        
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return False, set(), str(e)
        
        return False, set(), "Max retries exceeded"

    async def process_batch(self, session: ClientSession, urls: List[str]):
        """Process a batch of URLs concurrently"""
        tasks = []
        for url in urls:
            if url not in self.processed_urls:
                task = asyncio.create_task(self.process_url(session, url))
                tasks.append((url, task))
        
        for url, task in tasks:
            try:
                success, links, error = await task
                
                if success:
                    self.stats['successful'] += 1
                    self.stats['whatsapp_links'] += len(links)
                else:
                    self.stats['failed'] += 1
                    with open(self.error_file, 'a', newline='') as f:
                        csv.writer(f).writerow([
                            url, 'ERROR', error, self.max_retries
                        ])
                
                self.processed_urls.add(url)
                self.stats['processed'] += 1
                
            except Exception as e:
                self.stats['failed'] += 1
                with open(self.error_file, 'a', newline='') as f:
                    csv.writer(f).writerow([
                        url, 'EXCEPTION', str(e), self.max_retries
                    ])

# Continuing projectw/src/scraper.py

   async def run(self):
       """Main execution method"""
       try:
           # Load URLs from Excel
           logging.info(f"Loading URLs from {self.input_file}")
           df = pd.read_excel(self.input_file)
           urls = df.iloc[:, 0].tolist()
           self.stats['total_urls'] = len(urls)
           
           logging.info(f"Total URLs to process: {len(urls)}")
           if self.processed_urls:
               logging.info(f"Resuming from previous run. {len(self.processed_urls)} URLs already processed")
           
           # Process URLs in batches
           async with await self.create_session() as session:
               with tqdm(total=len(urls), desc="Processing URLs") as pbar:
                   for i in range(0, len(urls), self.batch_size):
                       batch = urls[i:i + self.batch_size]
                       await self.process_batch(session, batch)
                       
                       # Update progress bar
                       pbar.update(len(batch))
                       
                       # Save progress periodically
                       if i % 1000 == 0:
                           self._save_progress()
                           
                       # Log statistics
                       if i % 100 == 0:
                           self._log_stats()
           
           # Final progress save
           self._save_progress()
           
           # Log final statistics
           self._log_final_stats()
           
       except Exception as e:
           logging.error(f"Error during execution: {e}")
           raise
       finally:
           self._save_progress()

   def _log_stats(self):
       """Log current statistics"""
       elapsed = time.time() - self.stats['start_time']
       speed = self.stats['processed'] / elapsed if elapsed > 0 else 0
       
       logging.info(
           f"Progress: {self.stats['processed']}/{self.stats['total_urls']} "
           f"(Success: {self.stats['successful']}, "
           f"Failed: {self.stats['failed']}, "
           f"Links: {self.stats['whatsapp_links']}, "
           f"Speed: {speed:.2f} URLs/s)"
       )

   def _log_final_stats(self):
       """Log final statistics"""
       elapsed = time.time() - self.stats['start_time']
       
       logging.info("\n=== Scraping Summary ===")
       logging.info(f"Total URLs: {self.stats['total_urls']}")
       logging.info(f"Processed: {self.stats['processed']}")
       logging.info(f"Successful: {self.stats['successful']}")
       logging.info(f"Failed: {self.stats['failed']}")
       logging.info(f"WhatsApp Links Found: {self.stats['whatsapp_links']}")
       logging.info(f"Total Time: {elapsed:.2f} seconds")
       logging.info(f"Average Speed: {self.stats['processed']/elapsed:.2f} URLs/s")

def main():
   import argparse
   
   parser = argparse.ArgumentParser(description='WhatsApp Link Scraper')
   parser.add_argument('--input', required=True, help='Input Excel file')
   parser.add_argument('--output-dir', default='output', help='Output directory')
   parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing')
   parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')
   
   args = parser.parse_args()
   
   # Create scraper instance
   scraper = WebScraper(
       input_file=args.input,
       output_dir=args.output_dir,
       batch_size=args.batch_size,
       max_retries=args.max_retries
   )
   
   # Run scraper
   asyncio.run(scraper.run())

if __name__ == '__main__':
   main()
