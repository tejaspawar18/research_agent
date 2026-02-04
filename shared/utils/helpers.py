"""
Shared utilities for the Preventive Health Pipeline.
"""
import hashlib
import re
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import unicodedata
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)


def normalize_text(text: str) -> str:
    """Normalize text for comparison."""
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def compute_hash(content: str) -> str:
    """Compute SHA256 hash."""
    normalized = normalize_text(content)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def compute_url_hash(url: str) -> str:
    """Compute hash of normalized URL."""
    parsed = urlparse(url)
    # Normalize: lowercase host, remove trailing slashes, sort query params
    normalized = f"{parsed.scheme}://{parsed.netloc.lower()}{parsed.path.rstrip('/')}"
    if parsed.query:
        params = sorted(parsed.query.split('&'))
        normalized += '?' + '&'.join(params)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]


def extract_doi(text: str) -> Optional[str]:
    """Extract DOI from text."""
    pattern = r'10\.\d{4,}/[^\s<>"\']+'
    match = re.search(pattern, text)
    return match.group(0).rstrip('.,;') if match else None


def extract_pmid(url: str) -> Optional[str]:
    """Extract PMID from PubMed URL."""
    patterns = [
        r'pubmed\.ncbi\.nlm\.nih\.gov/(\d+)',
        r'pmc\.ncbi\.nlm\.nih\.gov/articles/PMC(\d+)',
        r'PMID[:\s]*(\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, url, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def clean_text(text: str) -> str:
    """Clean and normalize text content."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,;:!?\-\'\"()]', ' ', text)
    return text.strip()


def truncate_text(text: str, max_length: int = 1000) -> str:
    """Truncate text at word boundary."""
    if not text or len(text) <= max_length:
        return text or ""
    
    truncated = text[:max_length]
    last_space = truncated.rfind(' ')
    if last_space > max_length * 0.8:
        truncated = truncated[:last_space]
    
    return truncated + "..."


def title_similarity(title1: str, title2: str) -> float:
    """Calculate similarity between two titles using Jaccard."""
    t1_words = set(normalize_text(title1).split())
    t2_words = set(normalize_text(title2).split())
    
    if not t1_words or not t2_words:
        return 0.0
    
    intersection = len(t1_words & t2_words)
    union = len(t1_words | t2_words)
    
    return intersection / union if union > 0 else 0.0


def extract_sample_size(text: str) -> Optional[int]:
    """Extract sample size from text."""
    patterns = [
        r'n\s*=\s*(\d+[,\d]*)',
        r'(\d+[,\d]*)\s*participants',
        r'(\d+[,\d]*)\s*patients',
        r'(\d+[,\d]*)\s*subjects',
        r'sample\s*(?:size|of)\s*(\d+[,\d]*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            num_str = match.group(1).replace(',', '')
            try:
                return int(num_str)
            except ValueError:
                continue
    
    return None


def detect_study_type(text: str) -> str:
    """Detect study type from abstract/title."""
    text_lower = text.lower()
    
    if any(kw in text_lower for kw in ['meta-analysis', 'meta analysis', 'metaanalysis']):
        return 'meta_analysis'
    if any(kw in text_lower for kw in ['systematic review', 'systematic literature review']):
        return 'systematic_review'
    if any(kw in text_lower for kw in ['randomized controlled', 'randomised controlled', 'rct', 'randomized trial']):
        return 'rct'
    if any(kw in text_lower for kw in ['cohort study', 'cohort analysis', 'prospective study', 'longitudinal']):
        return 'cohort'
    if any(kw in text_lower for kw in ['case-control', 'case control']):
        return 'case_control'
    if any(kw in text_lower for kw in ['cross-sectional', 'cross sectional']):
        return 'cross_sectional'
    if any(kw in text_lower for kw in ['case study', 'case report', 'case series']):
        return 'case_study'
    if any(kw in text_lower for kw in ['mouse', 'mice', 'rat', 'animal model', 'murine']):
        return 'animal_study'
    if any(kw in text_lower for kw in ['in vitro', 'cell culture', 'cell line']):
        return 'in_vitro'
    if any(kw in text_lower for kw in ['commentary', 'editorial', 'opinion', 'perspective']):
        return 'commentary'
    
    return 'unknown'


def has_statistical_rigor(text: str) -> Dict[str, bool]:
    """Check for statistical rigor indicators."""
    text_lower = text.lower()
    
    return {
        'has_effect_sizes': any(kw in text_lower for kw in [
            'effect size', 'odds ratio', 'hazard ratio', 'risk ratio',
            'relative risk', 'cohen\'s d', 'hedges\' g'
        ]),
        'has_confidence_intervals': any(kw in text_lower for kw in [
            'confidence interval', '95% ci', '99% ci', 'ci:', 'ci ='
        ]),
        'has_p_values': bool(re.search(r'p\s*[<>=]\s*0\.\d+', text_lower)),
    }


def is_industry_funded(text: str) -> bool:
    """Check for industry funding indicators."""
    text_lower = text.lower()
    
    pharma_companies = [
        'pfizer', 'novartis', 'roche', 'merck', 'johnson & johnson',
        'abbvie', 'bristol-myers', 'eli lilly', 'amgen', 'gilead',
        'astrazeneca', 'sanofi', 'gsk', 'glaxosmithkline', 'bayer'
    ]
    
    funding_phrases = [
        'funded by', 'supported by', 'grant from', 'received funding',
        'financial support from', 'sponsored by'
    ]
    
    for company in pharma_companies:
        for phrase in funding_phrases:
            if company in text_lower and phrase in text_lower:
                return True
    
    return False


def has_bad_science_indicators(text: str) -> bool:
    """Check for bad science indicators."""
    from shared.models import BAD_SCIENCE_INDICATORS
    
    text_lower = text.lower()
    return any(indicator in text_lower for indicator in BAD_SCIENCE_INDICATORS)


def parse_date_string(date_str: str) -> Optional[datetime]:
    """Parse various date formats including RSS/Atom feed formats."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # Try RFC 2822 format first (common in RSS feeds)
    # e.g. "Mon, 03 Feb 2026 12:00:00 GMT"
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str)
    except (ValueError, TypeError):
        pass

    # Try ISO 8601 with timezone offset (e.g. "2026-02-03T12:00:00+00:00")
    try:
        # Handle timezone-aware ISO strings
        if '+' in date_str[10:] or date_str.endswith('Z'):
            clean = date_str.replace('Z', '+00:00')
            return datetime.fromisoformat(clean)
    except (ValueError, TypeError, IndexError):
        pass

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%Y-%m",
        "%B %Y",
        "%b %Y",
        "%Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, AttributeError):
            continue

    return None


def is_recent(dt: datetime, days: int = 7) -> bool:
    """Check if datetime is within specified days."""
    if not dt:
        return False
    cutoff = datetime.utcnow() - timedelta(days=days)
    return dt >= cutoff


def chunk_list(lst: list, chunk_size: int) -> List[list]:
    """Split list into chunks."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def safe_get(d: dict, *keys, default=None):
    """Safely get nested dictionary value."""
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d if d is not None else default


def get_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower().replace('www.', '')
    except Exception:
        return ""


def classify_source_quality(url: str, source_name: str = "") -> str:
    """Classify source quality based on URL and name."""
    from shared.models import HIGH_QUALITY_SOURCES
    
    domain = get_domain(url)
    combined = f"{domain} {source_name}".lower()
    
    # High quality sources
    if any(src in combined for src in HIGH_QUALITY_SOURCES):
        return "high"
    
    # Medium quality - academic and expert
    medium_indicators = [
        'harvard', 'yale', 'stanford', 'mit', 'oxford', 'cambridge',
        'university', 'edu', 'attia', 'medscape', 'webmd'
    ]
    if any(ind in combined for ind in medium_indicators):
        return "medium"
    
    return "low"