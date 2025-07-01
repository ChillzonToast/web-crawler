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
    def __init__(self, max_concurrent=5, delay_between_requests=1.0, output_file="crawled_data.json"):
        self.max_concurrent = max_concurrent
        self.delay_between_requests = delay_between_requests
        self.output_file = output_file
        self.visited_urls: Set[str] = set()
        self.crawled_data: List[Dict] = []
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Load existing data if file exists
        self.load_existing_data()
    
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
    
    def save_data(self):
        """Save current data to JSON file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.crawled_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Data saved to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
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
                    
                    logger.info(f"Successfully crawled and analyzed: {url}")
                    
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
    
    async def crawl_multiple(self, start_urls: List[str], max_pages: int = 100):
        """Crawl multiple URLs with breadth-first approach"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session
            
            urls_to_crawl = list(start_urls)
            crawled_count = 0
            
            while urls_to_crawl and crawled_count < max_pages:
                # Take batch of URLs
                current_batch = urls_to_crawl[:self.max_concurrent]
                urls_to_crawl = urls_to_crawl[self.max_concurrent:]
                
                # Filter out already visited URLs
                current_batch = [url for url in current_batch if url not in self.visited_urls]
                
                if not current_batch:
                    continue
                
                # Crawl batch concurrently
                tasks = [self.crawl_url(url) for url in current_batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                new_links = []
                for result in results:
                    if isinstance(result, dict) and result:
                        crawled_count += 1
                        # Add new links to queue
                        if "extracted_links" in result:
                            new_links.extend(result["extracted_links"])
                
                # Add new unique links to crawl queue
                for link in new_links:
                    if link not in self.visited_urls and link not in urls_to_crawl:
                        # Simple same-domain filter (optional)
                        if len(start_urls) == 1:
                            start_domain = urlparse(start_urls[0]).netloc
                            link_domain = urlparse(link).netloc
                            if start_domain == link_domain:
                                urls_to_crawl.append(link)
                        else:
                            urls_to_crawl.append(link)
                
                logger.info(f"Crawled: {crawled_count}, Queue: {len(urls_to_crawl)}")
                
                if crawled_count % 10 == 0:
                    logger.info(f"Progress: {crawled_count} pages crawled")

# Usage example
async def main():
    crawler = WebCrawlerAI(max_concurrent=5, delay_between_requests=1.0)
    
    # Starting URLs
    start_urls = [
        "https://nitc.ac.in/",
        "https://minerva.nitc.ac.in/",
        "https://athena.nitc.ac.in/"
    ]
    
    await crawler.crawl_multiple(start_urls, max_pages=50)
    
    logger.info(f"Crawling completed. Total pages: {len(crawler.crawled_data)}")

if __name__ == "__main__":
    asyncio.run(main())
