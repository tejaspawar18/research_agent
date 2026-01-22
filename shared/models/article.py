"""
Shared data models for the Preventive Health Research Pipeline.
"""
from datetime import datetime, date
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, HttpUrl
import uuid


class SourceQuality(str, Enum):
    """Source quality tiers based on guardrails."""
    HIGH = "high"           # Peer-reviewed: Nature, Lancet, Cell, BMJ, etc.
    MEDIUM = "medium"       # Expert commentary: Harvard, Yale, Peter Attia
    LOW = "low"             # News, blogs, preprints


class StudyType(str, Enum):
    """Research study types."""
    META_ANALYSIS = "meta_analysis"
    SYSTEMATIC_REVIEW = "systematic_review"
    RCT = "rct"
    COHORT = "cohort"
    CASE_CONTROL = "case_control"
    CROSS_SECTIONAL = "cross_sectional"
    CASE_STUDY = "case_study"
    ANIMAL_STUDY = "animal_study"
    IN_VITRO = "in_vitro"
    COMMENTARY = "commentary"
    NEWS = "news"
    UNKNOWN = "unknown"


class ProjectArea(str, Enum):
    """Main project areas."""
    DISEASE_PREVENTION = "disease_prevention"           # Project 1
    BEHAVIORAL_PROTOCOLS = "behavioral_protocols"       # Project 2
    NUTRITIONAL_PROTOCOLS = "nutritional_protocols"     # Project 3
    GOVERNMENT_INTERVENTIONS = "government_interventions"  # Project 4
    YOUTH_HEALTH = "youth_health"                       # Project 5
    GENERAL = "general"


class SourceType(str, Enum):
    """Types of sources."""
    JOURNAL = "journal"
    DATABASE = "database"
    GOVERNMENT = "government"
    ACADEMIC = "academic"
    EXPERT_BLOG = "expert_blog"
    NEWS = "news"
    ORGANIZATION = "organization"


class CrawlMethod(str, Enum):
    """Methods for crawling sources."""
    RSS = "rss"
    ATOM = "atom"
    HTML_SCRAPE = "html_scrape"
    API = "api"
    PUBMED_API = "pubmed_api"
    SCIENCEDIRECT_SCRAPE = "sciencedirect_scrape"
    ADAPTIVE = "adaptive"  # Intelligent multi-method fallback crawler


class ArticleStatus(str, Enum):
    """Article processing status."""
    CRAWLED = "crawled"
    DEDUPLICATED = "deduplicated"
    FILTERED = "filtered"
    FILTERED_OUT = "filtered_out"
    PROCESSED = "processed"
    NOTIFIED = "notified"
    FAILED = "failed"


class Author(BaseModel):
    """Author information."""
    name: str
    affiliation: Optional[str] = None
    orcid: Optional[str] = None


class SourceConfig(BaseModel):
    """Configuration for a data source."""
    source_id: str
    name: str
    url: str
    source_type: SourceType
    quality_tier: SourceQuality
    crawl_method: CrawlMethod
    rate_limit: int = 10  # requests per minute
    enabled: bool = True
    
    # Source-specific config
    rss_url: Optional[str] = None
    api_endpoint: Optional[str] = None
    css_selectors: Optional[Dict[str, str]] = None
    search_terms: Optional[List[str]] = None
    
    # Metadata
    last_crawled: Optional[datetime] = None
    
    class Config:
        use_enum_values = True


class Article(BaseModel):
    """Research article model."""
    article_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # Source info
    source_id: str
    source_name: Optional[str] = None
    url: str
    
    # Core metadata
    title: str
    authors: List[Author] = Field(default_factory=list)
    abstract: Optional[str] = None
    full_text: Optional[str] = None
    published_date: Optional[date] = None
    
    # Identifiers
    doi: Optional[str] = None
    pmid: Optional[str] = None

    # PDF information
    pdf_url: Optional[str] = None
    has_pdf: bool = False

    # Classification
    project_area: Optional[ProjectArea] = None
    sub_topic: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    
    # Quality assessment
    source_quality: Optional[SourceQuality] = None
    study_type: Optional[StudyType] = None
    evidence_level: Optional[int] = None  # 1-5 scale
    relevance_score: Optional[float] = None
    sample_size: Optional[int] = None
    
    # Quality flags
    has_effect_sizes: Optional[bool] = None
    has_confidence_intervals: Optional[bool] = None
    has_p_values: Optional[bool] = None
    is_industry_funded: Optional[bool] = None
    
    # LLM outputs
    summary: Optional[str] = None
    key_findings: List[str] = Field(default_factory=list)
    preventive_implications: Optional[str] = None
    quality_assessment: Optional[str] = None
    
    # Processing metadata
    content_hash: Optional[str] = None
    url_hash: Optional[str] = None
    title_hash: Optional[str] = None
    status: ArticleStatus = ArticleStatus.CRAWLED
    
    # Timestamps
    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    notified_at: Optional[datetime] = None
    
    # Errors
    error_message: Optional[str] = None
    
    class Config:
        use_enum_values = True


class CrawlJob(BaseModel):
    """Represents a crawl job for a source."""
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str
    source_name: str
    status: str = "pending"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    articles_found: int = 0
    articles_new: int = 0
    error_message: Optional[str] = None


class PipelineRun(BaseModel):
    """Represents a complete pipeline run."""
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_date: date = Field(default_factory=date.today)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    status: str = "running"
    
    # Statistics
    articles_crawled: int = 0
    articles_deduplicated: int = 0
    articles_filtered: int = 0
    articles_filtered_out: int = 0
    articles_summarized: int = 0
    articles_notified: int = 0
    
    # By project area
    by_project: Dict[str, int] = Field(default_factory=dict)
    
    # Errors
    errors: List[str] = Field(default_factory=list)


class DeduplicationResult(BaseModel):
    """Result of deduplication check."""
    is_duplicate: bool
    duplicate_of: Optional[str] = None
    match_type: Optional[str] = None  # url_hash, content_hash, title_similarity, doi, pmid
    similarity_score: Optional[float] = None


class QualityFilterResult(BaseModel):
    """Result of quality filtering."""
    passed: bool
    source_quality: SourceQuality
    study_type: StudyType
    evidence_level: int
    reasons: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class LLMClassificationResult(BaseModel):
    """Result of LLM classification."""
    project_area: ProjectArea
    sub_topic: str
    confidence: float
    keywords: List[str]
    reasoning: Optional[str] = None


class LLMSummaryResult(BaseModel):
    """Result of LLM summarization."""
    summary: str
    key_findings: List[str]
    preventive_implications: str
    quality_assessment: str
    relevance_score: float


class SlackMessage(BaseModel):
    """Slack message model."""
    channel: str
    article: Article
    message_type: str = "single"  # single, digest
    thread_ts: Optional[str] = None


class DailyDigest(BaseModel):
    """Daily digest for a project area."""
    project_area: ProjectArea
    channel: str
    date: date
    articles: List[Article]
    total_high_quality: int
    total_medium_quality: int


# Topic keyword mappings for classification
PROJECT_KEYWORDS = {
    ProjectArea.DISEASE_PREVENTION: {
        "atherosclerosis": ["heart", "atherosclerosis", "plaque", "lipid", "calcification", "cholesterol", 
                           "LDL", "HDL", "endothelial", "statin", "pcsk9", "lipoprotein", "ApoB", "ApoA",
                           "triglyceride", "coronary", "cardiology"],
        "diabetes": ["insulin", "glucose", "beta cell", "glp-1", "metformin", "hyperinsulinemia", 
                    "prediabetes", "hba1c", "c-peptide", "type 2 diabetes"],
        "metabolic": ["blood pressure", "liver", "fatty liver", "kidney", "cirrhosis", "fibrosis",
                     "albuminuria", "creatinine", "hypertension", "NAFLD", "DKD"],
        "cancer": ["carcinogenic", "tumor", "immunotherapy", "leukocytes", "genetic instability",
                  "genomic instability", "t-cell", "oncogenes", "oncologist"],
        "musculoskeletal": ["muscles", "joints", "bones", "sarcopenia", "osteoporosis", "bone mass",
                           "muscle mass", "cartilage", "tendon", "ligament", "myokines", "osteoblast",
                           "osteoclast", "orthopedic"],
        "neurodegenerative": ["dementia", "parkinson", "alzheimer", "huntington", "brain", 
                             "neurotransmitters", "cortex", "lobe", "memory", "motor", "nervous system",
                             "neuroinflammation", "lewy bodies"],
        "mental_health": ["anxiety", "depression", "mood", "brain", "endocrine", "psychology",
                         "serotonin", "dopamine", "stress"],
        "foundational": ["immune system", "immunity", "gut microbiome", "microbiota", "skin barrier",
                        "retina", "teeth", "oral microbiome", "gut", "oral", "skin", "eye", "ear"],
    },
    ProjectArea.BEHAVIORAL_PROTOCOLS: {
        "cardiovascular_exercise": ["aerobic", "anaerobic", "heart rate", "endurance", "cardiac output"],
        "resistance_training": ["hypertrophy", "muscle", "strength", "fast twitch", "resistance"],
        "stability_mobility": ["balance", "flexibility", "motor coordination", "yoga", "mobility"],
        "sleep": ["circadian rhythm", "REM", "non-REM", "sleep architecture", "chronotype", 
                 "slow wave", "sleep environment"],
        "meditation": ["breathing", "breathwork", "meditation", "mindfulness"],
        "emerging": ["naturotherapy", "heat exposure", "cold exposure", "infrared", "cryotherapy",
                    "HBOT", "hydrotherapy", "sauna", "acupuncture"],
        "risky_behaviors": ["tobacco", "smoking", "vaping", "alcohol", "drugs", "digital addiction",
                           "opioids", "nicotine", "addiction"],
    },
    ProjectArea.NUTRITIONAL_PROTOCOLS: {
        "protein": ["amino acid", "plant protein", "animal protein", "whey", "protein"],
        "carbohydrates": ["glycemic index", "simple carbs", "complex carbs", "fructose", "sucrose",
                         "refined carbs", "starch", "sugar"],
        "fats": ["saturated fat", "unsaturated fat", "PUFA", "MUFA", "omega-6", "trans fat", "oils"],
        "hydration": ["electrolyte", "water", "sodium", "osmosis", "fluids", "mineral absorption",
                     "dehydration"],
        "diets": ["fasting", "mediterranean", "low-carb", "vegan", "vegetarian", "intermittent",
                 "keto", "paleo"],
        "micronutrients": ["vitamin", "mineral", "iron", "calcium", "multivitamin"],
        "supplements": ["omega-3", "fibre", "magnesium", "creatine", "NAD", "ashwagandha", "herbal",
                       "food fortification"],
    },
    ProjectArea.GOVERNMENT_INTERVENTIONS: {
        "food_safety": ["food labelling", "food scoring", "food adulteration", "food safety", 
                       "food quality"],
        "air_pollution": ["PM 2.5", "PM 10", "COPD", "ambient air", "emission", "alveoli",
                         "hazardous air", "respiratory"],
        "water_pollution": ["waterborne", "fecal", "e. coli", "diarrhea", "water pollution",
                           "gastrointestinal"],
        "toxins": ["microplastics", "PFAs", "phthalates", "heavy metals", "bioaccumulation",
                  "toxicology"],
        "communications": ["health literacy", "health communication", "behavioral change", "nudge",
                          "campaigns", "outreach", "mobilisation"],
        "funding": ["insurance", "funding", "investment", "finance", "budget", 
                   "public health expenditure", "OOPE"],
        "workforce": ["preventive care training", "workforce training", "curriculum", 
                     "capacity building"],
    },
    ProjectArea.YOUTH_HEALTH: {
        "general": ["school", "students", "college", "adolescent health", "mental health", "obesity",
                   "child nutrition", "development", "health curriculum", "school intervention",
                   "college intervention", "student wellness", "campus health", "youth health"],
    },
}

# High-quality source indicators
HIGH_QUALITY_SOURCES = [
    "nature", "lancet", "cell", "bmj", "jaha", "pubmed", "pmc", "ihme",
    "cdc", "ecdc", "nih", "sciencedirect"
]

# Bad science indicators
BAD_SCIENCE_INDICATORS = [
    "miracle", "breakthrough cure", "doctors hate", "one weird trick",
    "100% effective", "no side effects", "instant results", "guaranteed"
]