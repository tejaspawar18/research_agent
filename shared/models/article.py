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
    """Article processing status - tracks pipeline progression.

    Flow: CRAWLED → UNIQUE → EXTRACTED → PROCESSED → NOTIFIED

    - CRAWLED: Article found and stored in DB
    - UNIQUE: Passed deduplication (not a duplicate URL/title/DOI)
    - EXTRACTED: Full text successfully extracted
    - PROCESSED: LLM summarization and classification complete
    - NOTIFIED: Sent to Slack channel
    - FILTERED_OUT: Excluded by quality filter (won't be processed)
    - FAILED: Error during processing (can be retried)
    """
    CRAWLED = "crawled"
    UNIQUE = "unique"
    EXTRACTED = "extracted"      # Full text extracted
    PROCESSED = "processed"      # LLM summarized and classified
    NOTIFIED = "notified"        # Sent to Slack
    FILTERED_OUT = "filtered_out"
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
# Project 1: Preventing key diseases impacting physical, cognitive and emotional health
# Project 2: Leveraging behavioural protocols for improving healthspan
# Project 3: Leveraging nutritional protocols for improving healthspan
# Project 4: Key government interventions for promoting preventive approaches to public health
# Project 5: Preparing youth in schools and colleges for improved future healthspan

PROJECT_KEYWORDS = {
    ProjectArea.DISEASE_PREVENTION: {
        "Preventing Atherosclerotic Heart Disease": [
            "heart", "atherosclerosis", "plaque", "lipid", "calcification", "cholesterol",
            "LDL", "HDL", "endothelial dysfunction", "statin", "pcsk9", "inhibitor",
            "lipoprotein", "ApoB", "ApoA", "triglyceride", "coronary artery", "cardiology"
        ],
        "Preventing Type 2 Diabetes and Insulin Resistance": [
            "insulin", "glucose", "beta cell", "glp-1", "metformin", "hyperinsulinemia",
            "prediabetes", "hba1c", "c-peptide", "type 2 diabetes", "insulin resistance"
        ],
        "Preventing Other Key Metabolic Diseases": [
            "blood pressure", "liver", "fatty liver", "kidney", "cirrhosis", "fibrosis",
            "albuminuria", "creatinine", "hypertension", "NAFLD", "DKD", "diabetology"
        ],
        "Preventing Cancers": [
            "carcinogenic", "tumor", "immunotherapy", "leukocytes", "genetic instability",
            "genomic instability", "t-cell", "oncogenes", "oncologist", "cancer"
        ],
        "Preventing Musculoskeletal Diseases": [
            "muscles", "joints", "bones", "sarcopenia", "osteoporosis", "bone mass",
            "muscle mass", "body mineral density", "cartilage", "tendon", "ligament",
            "myokines", "type 1 muscle fibres", "type 2 muscle fibres", "osteoblast",
            "osteoclast", "gait mechanics", "orthopedic"
        ],
        "Preventing Neurodegenerative Diseases": [
            "dementia", "parkinson", "alzheimer", "huntington", "brain", "neurotransmitters",
            "cortex", "lobe", "memory", "motor", "nervous system", "neuroinflammation",
            "lewy bodies"
        ],
        "Preventing Mental Health Conditions": [
            "anxiety", "depression", "mood", "brain", "endocrine", "neurotransmitters",
            "psychology", "serotonin", "dopamine", "stress", "mental health"
        ],
        "Maintaining Foundational Health": [
            "immune system", "innate immunity", "adaptive immunity", "gut microbiome",
            "microbiota", "skin barrier", "retina", "teeth", "tongue", "oral microbiome",
            "immunity", "gut", "oral", "skin", "eye", "ear"
        ],
    },
    ProjectArea.BEHAVIORAL_PROTOCOLS: {
        "Using Cardiovascular Exercises": [
            "aerobic training", "anaerobic exercise", "heart rate", "heart activity",
            "endurance", "cardiac output", "cardiovascular"
        ],
        "Using Resistance Training Exercises": [
            "hypertrophy", "muscle", "strength", "fast twitch", "resistance training",
            "weight training"
        ],
        "Using Stability and Mobility Exercises": [
            "balance", "flexibility", "motor coordination", "yoga", "stability", "mobility"
        ],
        "Sleep": [
            "circadian rhythm", "REM", "non-REM", "sleep architecture", "chronotype",
            "slow wave", "sleep environment", "sleep"
        ],
        "Meditation, Breathwork and Related Practices": [
            "breathing", "breathwork", "meditation", "mindfulness"
        ],
        "Emerging Behavioural Protocols": [
            "naturotherapy", "heat exposure", "cold exposure", "infra-red exposure",
            "cryotherapy", "HBOT", "hydrotherapy", "sauna", "acupuncture"
        ],
        "Risky Behaviours": [
            "tobacco", "smoking", "vaping", "alcohol", "drugs", "digital addiction",
            "opioids", "nicotine", "addiction", "dopamine"
        ],
    },
    ProjectArea.NUTRITIONAL_PROTOCOLS: {
        "Protein": [
            "amino acid", "plant protein", "animal protein", "whey", "protein"
        ],
        "Sugar and Carbohydrates": [
            "glycemic index", "simple carbs", "complex carbs", "fructose", "sucrose",
            "refined carbs", "starch", "sugar", "carbohydrates"
        ],
        "Fats and Oils": [
            "saturated fat", "unsaturated fat", "PUFA", "MUFA", "omega-6", "trans fat",
            "oils", "fats"
        ],
        "Hydration and Salts": [
            "electrolyte", "water", "sodium", "osmosis", "fluids", "mineral absorption",
            "dehydration", "hydration"
        ],
        "Comparison of Popular Diets and Dietary Techniques": [
            "fasting", "mediterranean", "low-carb", "vegan", "vegetarian", "intermittent",
            "keto", "paleo", "diet"
        ],
        "Micronutrients and Conventional Supplements": [
            "vitamin", "mineral", "gummies", "iron", "calcium", "multivitamin"
        ],
        "Emerging Supplements": [
            "omega-3", "fibre", "magnesium", "creatine", "NAD", "ashwagandha", "herbal",
            "food fortification", "supplements"
        ],
    },
    ProjectArea.GOVERNMENT_INTERVENTIONS: {
        "Improving Nutritional Standards and Food Safety": [
            "food labelling", "food scoring", "food adulteration", "food safety", "food quality"
        ],
        "Preventing Respiratory Infections by Tackling Air Pollution": [
            "PM 2.5", "PM 10", "COPD", "ambient air", "emission", "alveoli",
            "hazardous air", "air pollution", "respiratory"
        ],
        "Preventing Gastrointestinal Infections by Tackling Water Pollution": [
            "waterborne", "fecal", "e. coli", "diarrhea", "water pollution",
            "gastrointestinal"
        ],
        "Reducing Exposure to Key Toxins": [
            "microplastics", "PFAs", "phthalates", "heavy metals", "bioaccumulation",
            "toxicology", "toxins"
        ],
        "Driving Mass Behavioural Change Through Effective Public Health Communications": [
            "health literacy", "health communication", "behavioural change", "nudge",
            "campaigns", "outreach", "mobilisation"
        ],
        "Increasing Funding of Preventive Approaches to Public Health": [
            "insurance", "funding", "investment", "finance", "budget",
            "public health expenditure", "OOPE"
        ],
        "Adapting Healthcare Professional Talent Base": [
            "preventive care training", "workforce training", "curriculum",
            "capacity building"
        ],
    },
    ProjectArea.YOUTH_HEALTH: {
        "School and College Health Programs": [
            "school", "students", "college", "adolescent health", "mental health", "obesity",
            "child nutrition", "development", "health curriculum", "school intervention",
            "college intervention", "student wellness", "campus health", "youth health governance"
        ],
        "Regional Youth Health Initiatives": [
            "India", "USA", "UK", "EU", "Australia", "Scandinavia", "Japan", "Singapore",
            "China", "Canada", "South Korea", "France", "Germany"
        ],
    },
}

# Human-readable sub-topic display names (for Slack tags)
SUB_TOPIC_TAGS = {
    # Disease Prevention
    "Preventing Atherosclerotic Heart Disease": "Heart Disease",
    "Preventing Type 2 Diabetes and Insulin Resistance": "Diabetes",
    "Preventing Other Key Metabolic Diseases": "Metabolic Diseases",
    "Preventing Cancers": "Cancer",
    "Preventing Musculoskeletal Diseases": "Musculoskeletal",
    "Preventing Neurodegenerative Diseases": "Neurodegenerative",
    "Preventing Mental Health Conditions": "Mental Health",
    "Maintaining Foundational Health": "Foundational Health",
    # Behavioral Protocols
    "Using Cardiovascular Exercises": "Cardio Exercise",
    "Using Resistance Training Exercises": "Resistance Training",
    "Using Stability and Mobility Exercises": "Stability/Mobility",
    "Sleep": "Sleep",
    "Meditation, Breathwork and Related Practices": "Meditation/Breathwork",
    "Emerging Behavioural Protocols": "Emerging Protocols",
    "Risky Behaviours": "Risky Behaviours",
    # Nutritional Protocols
    "Protein": "Protein",
    "Sugar and Carbohydrates": "Carbohydrates",
    "Fats and Oils": "Fats/Oils",
    "Hydration and Salts": "Hydration",
    "Comparison of Popular Diets and Dietary Techniques": "Diets",
    "Micronutrients and Conventional Supplements": "Micronutrients",
    "Emerging Supplements": "Supplements",
    # Government Interventions
    "Improving Nutritional Standards and Food Safety": "Food Safety",
    "Preventing Respiratory Infections by Tackling Air Pollution": "Air Pollution",
    "Preventing Gastrointestinal Infections by Tackling Water Pollution": "Water Pollution",
    "Reducing Exposure to Key Toxins": "Toxins",
    "Driving Mass Behavioural Change Through Effective Public Health Communications": "Public Health Comms",
    "Increasing Funding of Preventive Approaches to Public Health": "Health Funding",
    "Adapting Healthcare Professional Talent Base": "Healthcare Workforce",
    # Youth Health
    "School and College Health Programs": "School/College Health",
    "Regional Youth Health Initiatives": "Regional Initiatives",
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