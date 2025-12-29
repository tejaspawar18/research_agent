"""
Article worker: single-article end-to-end processing.
Steps:
 - Download PDF (or extract HTML)
 - Save PDF locally, upload to S3
 - Summarize (PDF or HTML)
 - Generate embedding
 - Write to Scylla
 - Send vector to Qdrant
Retries included via tenacity in specific steps.
"""

import os
import uuid
import json
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from core.logger import get_logger
from extraction import download_pdf, extract_html_content
from core.file_manager import save_local_pdf, upload_s3
from processing.summarizer import summarize_pdf, summarize_html
from processing.embedding_generator import embed_text
from processing.taxonomy_classifier import classify_topics
from storage.write_scylla import write_paper, write_embedding_meta
from storage.qdrant_client import send_embedding

logger = get_logger("article_worker")

# Small wrapper used by orchestrator. Returns dict with status and optional paper_id / error
def process_article(item, scylla_session=None):
    """
    item: {
      "url": "...",
      "source": "...",
      "title": ...,
      "published": datetime,
      "selectors": {...}
    }
    """
    url = item.get("url")
    source = item.get("source")
    selectors = item.get("selectors") or {}
    print(f"Processing article: {url} from source: {source}")
    try:
        # 1. Try to download PDF
        pdf_result = download_pdf(url, selectors=selectors, outdir="data/temp")
        pdf_path = pdf_result.get("pdf_path") if isinstance(pdf_result, dict) else pdf_result
        
        local_pdf = None
        s3_url = None

        # 2. If pdf found -> save local and upload s3
        if pdf_path and isinstance(pdf_path, str) and os.path.exists(pdf_path):
            local_pdf = save_local_pdf(pdf_path)
            # use a stable s3 key prefix per source
            key_prefix = f"papers/{source}"
            s3_url = upload_s3(local_pdf, key_prefix)
            # Summarize PDF
            summary = summarize_pdf(local_pdf)
        else:
            # Fallback: extract HTML and summarize
            html_data = extract_html_content(url)
            if not html_data:
                raise RuntimeError("No PDF and no HTML content extracted")
            summary = summarize_html(html_data)
            # For HTML fallback, optionally save the HTML as a .txt locally and upload
            fname = f"data/pdfs/{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}.html"
            os.makedirs(os.path.dirname(fname), exist_ok=True)
            with open(fname, "w", encoding="utf-8") as f:
                f.write(html_data.get("text") or "")
            local_pdf = fname
            s3_url = upload_s3(local_pdf, f"papers/{source}")

        if not summary:
            raise RuntimeError("Summarizer returned no valid summary")

        # 3. Prepare metadata and store in Scylla
        paper_id = str(uuid.uuid4())
        extra = {
            "source": source,
            "published_date": item.get("published"),
            "journal": summary.get("journal") or None
        }

        # Write summary to scylla
        # write_paper(scylla_session, paper_id, summary, local_pdf, s3_url, extra=extra)

        # 4. Embeddings
        # emb_text = summary.get("final_summary") or summary.get("key_findings") or summary.get("abstract") or ""
        # embedding = embed_text(emb_text)
        # topics = classify_topics(emb_text)
        # # send embedding to Qdrant ingest
        # qresp = send_embedding(paper_id, embedding, {
        #     "title": summary.get("title"),
        #     "source": source,
        #     "topics": topics
        # })
        # # store embedding meta
        # write_embedding_meta(scylla_session, paper_id, qresp.get("vector_id") if isinstance(qresp, dict) else None, topics)

        return {"status": "ok", "paper_id": paper_id}
    except Exception as e:
        logger.exception(f"Error processing article {url}: {e}")
        return {"status": "error", "error": str(e)}
