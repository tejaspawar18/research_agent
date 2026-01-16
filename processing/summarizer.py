# processing/summarizer.py
import json
import os
import re
from datetime import datetime
from openai import OpenAI
from core.logger import get_logger
from processing.models import PaperSummary

from dotenv import load_dotenv

load_dotenv()

logger = get_logger("summarizer")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Directories for saving content
CONTENT_DIR = "data/paper_content"
SUMMARY_DIR = "data/summaries"

def _ensure_dirs():
    """Create output directories if they don't exist."""
    os.makedirs(CONTENT_DIR, exist_ok=True)
    os.makedirs(SUMMARY_DIR, exist_ok=True)

def _generate_filename(prefix="paper"):
    """Generate a unique filename with timestamp."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}"

def _strip_markdown_codeblocks(raw: str) -> str:
    """Strip markdown code blocks from LLM response."""
    raw = raw.strip()
    if raw.startswith("```"):
        # Remove opening ```json or ```
        raw = re.sub(r'^```(?:json)?\n?', '', raw)
        # Remove closing ```
        raw = re.sub(r'\n?```$', '', raw)
    return raw.strip()

def _save_content(text: str, source_type: str = "html", title: str = None) -> str:
    """Save raw paper content before summarization."""
    _ensure_dirs()
    filename = _generate_filename(f"{source_type}_content")
    filepath = os.path.join(CONTENT_DIR, f"{filename}.txt")
    
    with open(filepath, "w", encoding="utf-8") as f:
        if title:
            f.write(f"Title: {title}\n")
            f.write("=" * 60 + "\n\n")
        f.write(text)
    
    logger.info(f"Saved raw content → {filepath}")
    return filepath


def _save_summary(summary: dict, source_type: str = "html") -> str:
    """Save the summarized content as JSON."""
    _ensure_dirs()
    filename = _generate_filename(f"{source_type}_summary")
    filepath = os.path.join(SUMMARY_DIR, f"{filename}.json")
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"Saved summary → {filepath}")
    return filepath

SUMMARY_RULES = """
You are a biomedical research analyst.
Generate a STRICT JSON object with the following keys:

title
doi
published_date
authors
abstract
objective
methodology
dataset
model
experiments
key_findings
limitations
future_scope
final_summary


Rules:
- Final summary must be ≤ 600 words.
- dont change the abstract, title and authors.
- also dont change the doi.
- return the exact published date
- If a field is missing in the paper, put null.
- Do NOT include any text outside the JSON.
"""

def _extract_message_content(resp):
    """
    Safely extract the textual content from various OpenAI Python SDK shapes.
    Handles:
      - resp.choices[0].message.content (object attribute)
      - resp.choices[0].message["content"] (mapping)
      - resp.choices[0].text (older shape)
    """
    try:
        choice0 = resp.choices[0]
    except Exception:
        return None

    # 1) message attribute with content
    msg = getattr(choice0, "message", None)
    if msg is not None:
        # msg could be an object with .content or a dict-like
        content = getattr(msg, "content", None)
        if content is not None:
            return content
        # try mapping access
        try:
            return msg.get("content")
        except Exception:
            pass

    # 2) older shape: choice0.get("text") or choice0.text
    text = getattr(choice0, "text", None)
    if text:
        return text

    try:
        return choice0.get("text")
    except Exception:
        pass

    return None

from pypdf import PdfReader

# ... (omitted existing imports/code)

def summarize_pdf(file_path: str, title: str, doi: str, published_date: str) -> dict:
    """
    Summarize PDF using GPT-4o-mini with strict JSON output.
    Extracts text locally using pypdf.
    Saves the summary to data/summaries/.
    """
    logger.info(f"Summarizing PDF → {file_path}")


    # Extract text from PDF
    text_content = ""
    try:
        reader = PdfReader(file_path)
        for page in reader.pages:
            extract = page.extract_text()
            if extract:
                text_content += extract + "\n"
    except Exception as e:
        logger.error(f"Failed to extract text from PDF {file_path}: {e}")
        return None

    if not text_content.strip():
        logger.error(f"No text extracted from PDF {file_path}")
        return None

    # Save raw content (optional, but good for debugging/record)
    _save_content(text_content, source_type="pdf", title=os.path.basename(file_path))

    # Send text to OpenAI (same approach as summarize_html)
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user",
                 "content": text_content + "\n\n" + SUMMARY_RULES
                 }
            ],
            response_format={"type": "json_object"}
        )
    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        return None


    raw = _extract_message_content(response)
    tokens = response.usage
    if raw is None:
        logger.error("Could not extract content from OpenAI response.")
        return None

    # Strip markdown code blocks before parsing
    raw = _strip_markdown_codeblocks(raw)
    
    try:
        data = json.loads(raw)
        validated = PaperSummary(**data)
        result = validated.dict()
        if title:
            result["title"] = title
        if doi:
            result["doi"] = doi
        if published_date:
            result["published_date"] = published_date

        
        # Inject token usage if available
        if tokens:
            result["input_token"] = tokens.prompt_tokens
            result["output_token"] = tokens.completion_tokens

        # Save the summary
        _save_summary(result, source_type="pdf")
        
        return result
    except Exception as e:
        logger.error(f"JSON parsing failed: {e}")
        # Save failed raw output for debugging
        debug_path = f"data/debug/failed_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        os.makedirs("data/debug", exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(raw)
        logger.error(f"Raw output saved to {debug_path}")
        return None


def summarize_html(html_data: dict) -> dict:
    """
    Fallback summarization for HTML-only sources.
    Saves raw content before summarizing and saves the summary.
    """
    logger.info("Summarizing HTML fallback")

    if not isinstance(html_data, dict) or "text" not in html_data:
        logger.error("Invalid input to summarize_html: expected a dict with a 'text' key.")
        return None
    
    text = html_data.get("text")
    title = html_data.get("title")
    
    if not text:
        logger.error("No text found in html_data to summarize.")
        return None

    # Save raw content before summarizing
    _save_content(text, source_type="html", title=title)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": text + "\n\n" + SUMMARY_RULES}
        ],
        response_format={"type": "json_object"}
    )

    raw = _extract_message_content(response)
    tokens = response.usage
    if raw is None:
        logger.error("Could not extract content from OpenAI response (HTML).")
        return None

    # Strip markdown code blocks before parsing
    raw = _strip_markdown_codeblocks(raw)
    
    try:
        data = json.loads(raw)
        validated = PaperSummary(**data)
        result = validated.dict()
        
        # Inject token usage if available
        if tokens:
            result["input_token"] = tokens.prompt_tokens
            result["output_token"] = tokens.completion_tokens
        
        # Save the summary
        _save_summary(result, source_type="html")
        
        return result
    except Exception as e:
        logger.error(f"HTML JSON parsing failed: {e}")
        logger.error(f"Raw output was:\n{raw}")
        return None