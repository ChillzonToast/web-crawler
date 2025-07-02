import asyncio
import aiohttp
import json
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import re
from typing import Set, Dict, List
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebCrawlerAI:
    def __init__(self, max_concurrent=5, delay_between_requests=1.0, output_file="crawled_data.json", queue_file="crawl_queue.json"):
        self.max_concurrent = max_concurrent
        self.delay_between_requests = delay_between_requests
        self.output_file = output_file
        self.queue_file = queue_file
        self.visited_urls: Set[str] = set()
        self.crawled_data: List[Dict] = []
        self.urls_to_crawl: List[str] = []  # Persistent queue
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Load existing data and queue
        self.load_existing_data()
        self.load_queue()
    
    def load_existing_data(self):
        """Load previously crawled data from JSON file"""
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                self.crawled_data = json.load(f)
                # Rebuild visited URLs set
                self.visited_urls = {item['url'] for item in self.crawled_data}
            logger.info(f"Loaded {len(self.crawled_data)} existing records")
        except FileNotFoundError:
            logger.info("No existing data file found. Starting fresh.")
        except json.JSONDecodeError:
            logger.warning("Corrupted data file. Starting fresh.")
    
    def load_queue(self):
        """Load the crawl queue from JSON file"""
        try:
            with open(self.queue_file, 'r', encoding='utf-8') as f:
                self.urls_to_crawl = json.load(f)
            logger.info(f"Loaded {len(self.urls_to_crawl)} URLs from queue")
        except FileNotFoundError:
            logger.info("No existing queue file found. Starting with empty queue.")
        except json.JSONDecodeError:
            logger.warning("Corrupted queue file. Starting with empty queue.")
    
    def save_data(self):
        """Save current data to JSON file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                # Sort by sensitivity level (highest first)
                self.crawled_data.sort(key=lambda x: x['sensitivity_level'], reverse=True)
                json.dump(self.crawled_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Data saved to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def save_queue(self):
        """Save the current queue to JSON file"""
        try:
            with open(self.queue_file, 'w', encoding='utf-8') as f:
                json.dump(self.urls_to_crawl, f, indent=2, ensure_ascii=False)
            logger.info(f"Queue saved to {self.queue_file} ({len(self.urls_to_crawl)} URLs)")
        except Exception as e:
            logger.error(f"Error saving queue: {e}")
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text
    
    def extract_text_content(self, html: str, url: str) -> str:
        """Extract meaningful text content from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Get text content
            text = soup.get_text()
            
            # Clean the text
            text = self.clean_text(text)
            
            # Limit text length (optional - adjust as needed)
            if len(text) > 10000:
                text = text[:10000] + "..."
            
            return text
        except Exception as e:
            logger.error(f"Error extracting text from {url}: {e}")
            return ""
    
    def extract_links(self, html: str, base_url: str) -> List[str]:
        """Extract all links from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                
                # Basic URL validation
                parsed = urlparse(full_url)
                if parsed.scheme in ['http', 'https'] and parsed.netloc:
                    links.append(full_url)
            
            return links
        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {e}")
            return []
    
    def should_crawl_url(self, url: str, allowed_domains: List[str] = None) -> bool:
        """Check if URL should be crawled based on domain filtering"""
        if not allowed_domains:
            return True
        
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        return any(allowed_domain.lower() in domain for allowed_domain in allowed_domains)
    
    async def analyze_with_ai(self, content: str, url: str) -> Dict:
        """Analyze content with AI to get title, sensitivity, and summary"""
        try:
            # Load the prompt template
            with open("prompt.txt", "r", encoding='utf-8') as f:
                prompt_template = f.read()
            
            # Format the prompt with the content
            prompt = prompt_template.format(
                url=url,
                content=content[:5000]  # Limit content length for AI processing
            )
            
            # Make async AI request
            async with self.session.post(
                "https://tools.originality.ai/tool-ai-prompt-generator/backend/generate.php",
                json={"prompt": prompt},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    ai_output = result.get("output", "")
                    
                    # Parse AI output (expecting structured format)
                    return self.parse_ai_output(ai_output)
                else:
                    logger.error(f"AI API returned status {response.status}")
                    return self.default_analysis()
        
        except asyncio.TimeoutError:
            logger.error("AI request timed out")
            return self.default_analysis()
        except Exception as e:
            logger.error(f"Error in AI analysis: {e}")
            return self.default_analysis()
    
    def parse_ai_output(self, ai_output: str) -> Dict:
        """Parse structured AI output"""
        try:
            # Try to extract structured information from AI output
            # This assumes AI returns in a specific format - adjust as needed
            lines = ai_output.strip().split('\n')
            
            title = "Untitled"
            sensitivity = 1
            summary = "No summary available"
            
            for line in lines:
                line = line.strip()
                if line.lower().startswith('title:'):
                    title = line[6:].strip()
                elif line.lower().startswith('sensitivity:'):
                    try:
                        sensitivity = int(re.search(r'\d+', line).group())
                        sensitivity = max(1, min(10, sensitivity))  # Clamp between 1-10
                    except:
                        sensitivity = 1
                elif line.lower().startswith('summary:'):
                    summary = line[8:].strip()
            
            return {
                "title": title,
                "sensitivity_level": sensitivity,
                "summary": summary
            }
        
        except Exception as e:
            logger.error(f"Error parsing AI output: {e}")
            return self.default_analysis()
    
    def default_analysis(self) -> Dict:
        """Return default analysis when AI fails"""
        return {
            "title": "Content Analysis Failed",
            "sensitivity_level": 1,
            "summary": "Unable to analyze content"
        }
    
    async def crawl_url(self, url: str) -> Dict:
        """Crawl a single URL and analyze its content"""
        async with self.semaphore:
            try:
                if url in self.visited_urls:
                    return None
                
                logger.info(f"Crawling: {url}")
                
                async with self.session.get(
                    url, 
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers={'User-Agent': 'Mozilla/5.0 (compatible; WebCrawler/1.0)'}
                ) as response:
                    
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} for {url}")
                        return None
                    
                    html = await response.text()
                    
                    # Extract text content
                    text_content = self.extract_text_content(html, url)
                    
                    if not text_content or len(text_content.strip()) < 50:
                        logger.info(f"Insufficient content in {url}")
                        return None
                    
                    # Analyze with AI
                    ai_analysis = await self.analyze_with_ai(text_content, url)
                    
                    # Create result
                    result = {
                        "url": url,
                        "crawled_at": datetime.now().isoformat(),
                        "content": text_content,
                        "title": ai_analysis["title"],
                        "sensitivity_level": ai_analysis["sensitivity_level"],
                        "summary": ai_analysis["summary"],
                        "content_length": len(text_content)
                    }
                    
                    # Mark as visited and save
                    self.visited_urls.add(url)
                    self.crawled_data.append(result)
                    self.save_data()  # Save after each successful crawl
                    
                    logger.info(f"Successfully crawled and analyzed: {url} (Sensitivity: {ai_analysis['sensitivity_level']})")
                    
                    # Extract links for further crawling
                    links = self.extract_links(html, url)
                    
                    # Add delay to be respectful
                    await asyncio.sleep(self.delay_between_requests)
                    
                    return {**result, "extracted_links": links}
            
            except asyncio.TimeoutError:
                logger.error(f"Timeout crawling {url}")
                return None
            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")
                return None
    
    async def crawl_endless(self, start_urls: List[str] = None, allowed_domains: List[str] = None):
        """Crawl URLs endlessly until manually stopped"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session
            
            # Add start URLs to queue if provided and queue is empty
            if start_urls and not self.urls_to_crawl:
                self.urls_to_crawl.extend(start_urls)
                logger.info(f"Added {len(start_urls)} start URLs to queue")
            
            crawled_count = len(self.visited_urls)  # Start from existing count
            
            try:
                while True:  # Endless loop
                    # Check if queue is empty
                    if not self.urls_to_crawl:
                        logger.warning("Queue is empty! Waiting for new URLs...")
                        await asyncio.sleep(10)
                        continue
                    
                    # Take batch of URLs from the front of queue
                    batch_size = min(self.max_concurrent, len(self.urls_to_crawl))
                    current_batch = self.urls_to_crawl[:batch_size]
                    self.urls_to_crawl = self.urls_to_crawl[batch_size:]  # Remove from queue
                    
                    # Filter out already visited URLs and domain restrictions
                    current_batch = [
                        url for url in current_batch 
                        if url not in self.visited_urls and self.should_crawl_url(url, allowed_domains)
                    ]
                    
                    if not current_batch:
                        continue
                    
                    # Crawl batch concurrently
                    tasks = [self.crawl_url(url) for url in current_batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results and add new links to queue
                    new_links = []
                    successful_crawls = 0
                    
                    for result in results:
                        if isinstance(result, dict) and result:
                            successful_crawls += 1
                            crawled_count += 1
                            # Add new links to queue
                            if "extracted_links" in result:
                                new_links.extend(result["extracted_links"])
                    
                    # Add new unique links to crawl queue
                    unique_new_links = []
                    for link in new_links:
                        if (link not in self.visited_urls and 
                            link not in self.urls_to_crawl and 
                            link not in unique_new_links and
                            self.should_crawl_url(link, allowed_domains)):
                            unique_new_links.append(link)
                    
                    self.urls_to_crawl.extend(unique_new_links)
                    
                    # Save queue periodically
                    if crawled_count % 10 == 0:
                        self.save_queue()
                    
                    # Log progress
                    logger.info(f"Crawled: {crawled_count}, Queue: {len(self.urls_to_crawl)}, New links: {len(unique_new_links)}")
                    
                    # Progress milestone
                    if crawled_count % 50 == 0:
                        logger.info(f"MILESTONE: {crawled_count} pages crawled, {len(self.urls_to_crawl)} URLs in queue")
                        # Save queue at milestones
                        self.save_queue()
            
            except KeyboardInterrupt:
                logger.info("Crawling interrupted by user")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            finally:
                # Always save queue before exiting
                self.save_queue()
                logger.info(f"Crawling stopped. Total crawled: {crawled_count}, Queue preserved: {len(self.urls_to_crawl)}")

# Usage example
async def main():
    crawler = WebCrawlerAI(max_concurrent=25, delay_between_requests=0)
    
    # Starting URLs (only needed if starting fresh)
    start_urls = [
        "https://nitc.ac.in/",
        "https://minerva.nitc.ac.in/",
        "https://athena.nitc.ac.in/"
    ]
    
    # Domain filtering to stay within specific domains
    allowed_domains = [
        "nitc.ac.in",
        "minerva.nitc.ac.in", 
        "athena.nitc.ac.in"
    ]
    
    # Start endless crawling
    await crawler.crawl_endless(start_urls, allowed_domains)

if __name__ == "__main__":
    asyncio.run(main())