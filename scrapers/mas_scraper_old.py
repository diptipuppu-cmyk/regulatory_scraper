import os
import json
from urllib.parse import urljoin
from datetime import datetime
from playwright.async_api import async_playwright

async def scrape_mas(config_path, raw_volume_path, run_id, start_time, check_processed_fn, log_audit_fn, get_hash_fn):
    """Executes MAS regulatory web scraping using SMBC two-step navigation rules."""
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    regulator = config["regulator_id"]
    start_url = config["base_url"]
    
    docs_found = 0
    docs_processed = 0
    docs_skipped = 0
    docs_failed = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            print(f"Navigating to {regulator} Banking Regulations page...")
            await page.goto(start_url, timeout=60000)
            await page.wait_for_load_state("networkidle")
            
            # Step 1: Open all regulations list
            view_all = page.locator("a", has_text="view all regulations")
            if await view_all.count() > 0:
                await view_all.first.click()
                await page.wait_for_load_state("networkidle")
                
            # Step 2: Apply 'Banking' sector filter
            banking_filter = page.locator("label:has-text('Banking')")
            if await banking_filter.count() > 0:
                await banking_filter.first.click()
                await page.wait_for_timeout(3000) 
            
            # Step 3: Extract the detail page URLs from the search results
            print("Extracting document detail pages from search results...")
            hrefs = await page.eval_on_selector_all("a[href]", "elements => elements.map(e => e.getAttribute('href'))")
            
            detail_urls = set()
            for href in hrefs:
                if href and not href.startswith("#") and "?" not in href:
                    if "/regulation/" in href or "/publications/" in href or "/news/" in href:
                        if "regulations-and-guidance" not in href and "search" not in href:
                            full_url = urljoin(start_url, href)
                            detail_urls.add(full_url)
                            
            print(f"Found {len(detail_urls)} article pages to inspect.")
            
            # Step 4: Navigate into each article page to find and download the PDFs
            for detail_url in list(detail_urls)[:5]: 
                try:
                    await page.goto(detail_url, timeout=60000)
                    await page.wait_for_load_state("networkidle")
                    
                    pdf_links = await page.query_selector_all("a[href$='.pdf']")
                    docs_found += len(pdf_links)
                    
                    for link in pdf_links:
                        doc_url = await link.get_attribute("href")
                        if doc_url.startswith("/"):
                            doc_url = urljoin(detail_url, doc_url)
                        
                        file_name = doc_url.split("/")[-1].split("?")[0]
                        save_path = f"{raw_volume_path}/{regulator}/{file_name}"
                        
                        try:
                            print(f"Downloading: {file_name}")
                            
                            # NEW FIX: Use Playwright's authenticated network context instead of urllib
                            response = await context.request.get(doc_url)
                            if response.status != 200:
                                raise Exception(f"Download blocked by MAS with HTTP {response.status}")
                            
                            # Read the actual PDF binary and write it to the Databricks volume
                            pdf_bytes = await response.body()
                            with open(save_path, "wb") as f:
                                f.write(pdf_bytes)
                                
                            content_hash = get_hash_fn(save_path)
                            file_size = os.path.getsize(save_path)
                            
                            if not check_processed_fn(content_hash):
                                docs_processed += 1
                                log_audit_fn(
                                    run_id=run_id, regulator_id=regulator, start_time_sgt=start_time, end_time_sgt=datetime.now(),
                                    execution_status="IN_PROGRESS", source_url=detail_url, document_url=doc_url, 
                                    file_name=file_name, file_size_bytes=file_size, content_hash=content_hash, processing_status="PROCESSED"
                                )
                                print(f" -> Processed: {file_name}")
                            else:
                                docs_skipped += 1
                                log_audit_fn(
                                    run_id=run_id, regulator_id=regulator, start_time_sgt=start_time, end_time_sgt=datetime.now(),
                                    execution_status="IN_PROGRESS", source_url=detail_url, document_url=doc_url, 
                                    file_name=file_name, file_size_bytes=file_size, content_hash=content_hash, processing_status="SKIPPED"
                                )
                                print(f" -> Skipped (duplicate): {file_name}")
                                os.remove(save_path)
                                
                        except Exception as doc_e:
                            docs_failed += 1
                            log_audit_fn(
                                run_id=run_id, regulator_id=regulator, start_time_sgt=start_time, end_time_sgt=datetime.now(),
                                execution_status="IN_PROGRESS", source_url=detail_url, document_url=doc_url, 
                                file_name=file_name, processing_status="FAILED", error_details=str(doc_e)
                            )
                except Exception as e:
                    print(f"Failed to load detail page {detail_url}: {e}")
            
            # Step 5: Record overall run outcome
            log_audit_fn(
                run_id=run_id, regulator_id=regulator, start_time_sgt=start_time, end_time_sgt=datetime.now(),
                execution_status="SUCCESS", docs_found=docs_found, docs_processed=docs_processed, 
                docs_skipped=docs_skipped, docs_failed=docs_failed, source_url=start_url
            )
            
        except Exception as e:
            print(f"Error scraping {regulator}: {e}")
            log_audit_fn(
                run_id=run_id, regulator_id=regulator, start_time_sgt=start_time, end_time_sgt=datetime.now(),
                execution_status="FAILED", docs_found=docs_found, docs_processed=docs_processed, 
                docs_skipped=docs_skipped, docs_failed=docs_failed, source_url=start_url, error_details=str(e)
            )
            
        finally:
            await browser.close()
            
    return docs_processed