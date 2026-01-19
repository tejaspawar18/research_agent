"""
LLM Service - AI-powered quality filtering, classification, and summarization.
"""
import logging
import os
import json
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

import sys
sys.path.insert(0, '/app')

from shared.models import (
    Article, ProjectArea, StudyType, SourceQuality,
    LLMClassificationResult, LLMSummaryResult, QualityFilterResult,
    PROJECT_KEYWORDS, BAD_SCIENCE_INDICATORS,
)
from shared.utils import (
    RedisManager, detect_study_type, extract_sample_size,
    has_statistical_rigor, is_industry_funded, has_bad_science_indicators,
    truncate_text,
)
from shared.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_manager = RedisManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    await redis_manager.connect()
    config.load()
    logger.info("LLM service started")
    yield
    await redis_manager.disconnect()
    logger.info("LLM service stopped")


app = FastAPI(
    title="LLM Service",
    description="AI-powered article quality filtering, classification, and summarization",
    version="1.0.0",
    lifespan=lifespan,
)


# Request/Response models
class FilterRequest(BaseModel):
    """Request to filter an article."""
    article: Article


class FilterResponse(BaseModel):
    """Response from quality filter."""
    article_id: str
    passed: bool
    source_quality: str
    study_type: str
    evidence_level: int
    reasons: List[str]
    warnings: List[str]


class ClassifyRequest(BaseModel):
    """Request to classify an article."""
    article: Article


class ClassifyResponse(BaseModel):
    """Response from classification."""
    article_id: str
    project_area: str
    sub_topic: str
    confidence: float
    keywords: List[str]


class SummarizeRequest(BaseModel):
    """Request to summarize an article."""
    article: Article


class SummarizeResponse(BaseModel):
    """Response from summarization."""
    article_id: str
    summary: str
    key_findings: List[str]
    preventive_implications: str
    quality_assessment: str
    relevance_score: float


class BatchRequest(BaseModel):
    """Batch processing request."""
    articles: List[Article]


class BatchResponse(BaseModel):
    """Batch processing response."""
    total: int
    passed_filter: int
    processed: int
    results: List[Dict[str, Any]]


# LLM Provider
class LLMProvider:
    """LLM provider interface."""
    
    def __init__(self):
        self.provider = os.getenv("LLM_PROVIDER", "openai").lower()
        self.api_key = os.getenv("LLM_API_KEY")
        self.model = os.getenv("LLM_MODEL", "gpt-4-turbo")
    
    async def complete(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = 1500,
        temperature: float = 0.3,
    ) -> str:
        """Generate completion."""
        if self.provider == "anthropic":
            return await self._anthropic_complete(messages, max_tokens, temperature)
        else:
            return await self._openai_complete(messages, max_tokens, temperature)
    
    async def _openai_complete(self, messages, max_tokens, temperature) -> str:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                },
                timeout=90.0,
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
    
    async def _anthropic_complete(self, messages, max_tokens, temperature) -> str:
        system_msg = ""
        user_msgs = []
        for msg in messages:
            if msg["role"] == "system":
                system_msg = msg["content"]
            else:
                user_msgs.append(msg)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "max_tokens": max_tokens,
                    "system": system_msg,
                    "messages": user_msgs,
                },
                timeout=90.0,
            )
            response.raise_for_status()
            data = response.json()
            return data["content"][0]["text"]


# Quality Filter
class QualityFilter:
    """
    Evidence-based quality filtering following the guardrails.
    
    HIGH Priority: Meta-analyses, RCTs, large cohorts from peer-reviewed sources
    MEDIUM Priority: Expert commentary, observational studies
    LOW Priority: Case studies, small samples, opinion pieces
    """
    
    def filter(self, article: Article) -> QualityFilterResult:
        """Apply quality filter to article."""
        reasons = []
        warnings = []
        
        # 1. Determine study type
        text = f"{article.title or ''} {article.abstract or ''}"
        study_type = detect_study_type(text)
        
        # 2. Check for bad science indicators
        if has_bad_science_indicators(text):
            reasons.append("Contains sensational/misleading language")
            return QualityFilterResult(
                passed=False,
                source_quality=article.source_quality or SourceQuality.LOW,
                study_type=StudyType(study_type),
                evidence_level=1,
                reasons=reasons,
                warnings=warnings,
            )
        
        # 3. Extract sample size
        sample_size = extract_sample_size(text)
        
        # 4. Check statistical rigor
        rigor = has_statistical_rigor(text)
        
        # 5. Check for industry funding
        if is_industry_funded(text):
            warnings.append("Potential industry funding detected")
        
        # 6. Calculate evidence level (1-5)
        evidence_level = self._calculate_evidence_level(
            study_type, sample_size, rigor, article.source_quality
        )
        
        # 7. Apply guardrails
        passed = True
        source_quality = article.source_quality or SourceQuality.MEDIUM
        
        # LOW priority filters
        if study_type in ['case_study', 'commentary', 'unknown']:
            if source_quality != SourceQuality.HIGH:
                reasons.append(f"Low-evidence study type: {study_type}")
                passed = evidence_level >= 2
        
        # Sample size checks
        if sample_size is not None:
            if study_type == 'rct' and sample_size < 50:
                reasons.append(f"Small RCT sample size: n={sample_size} (min: 50 per arm)")
                passed = False
            elif study_type in ['cohort', 'cross_sectional'] and sample_size < 150:
                warnings.append(f"Moderate sample size: n={sample_size} (recommended: >150)")
            elif sample_size < 20:
                reasons.append(f"Very small sample size: n={sample_size}")
                passed = False
        
        # Statistical rigor checks for quantitative studies
        if study_type in ['rct', 'cohort', 'meta_analysis']:
            if not rigor['has_effect_sizes'] and not rigor['has_confidence_intervals']:
                warnings.append("Missing effect sizes or confidence intervals")
        
        # Source quality overrides
        if source_quality == SourceQuality.HIGH:
            if study_type in ['meta_analysis', 'systematic_review', 'rct']:
                passed = True  # High-quality sources with strong study designs pass
                evidence_level = max(evidence_level, 4)
        
        return QualityFilterResult(
            passed=passed,
            source_quality=source_quality,
            study_type=StudyType(study_type),
            evidence_level=evidence_level,
            reasons=reasons,
            warnings=warnings,
        )
    
    def _calculate_evidence_level(
        self,
        study_type: str,
        sample_size: Optional[int],
        rigor: Dict[str, bool],
        source_quality: Optional[SourceQuality],
    ) -> int:
        """Calculate evidence level 1-5."""
        # Base level from study type
        type_levels = {
            'meta_analysis': 5,
            'systematic_review': 5,
            'rct': 4,
            'cohort': 3,
            'case_control': 3,
            'cross_sectional': 2,
            'case_study': 1,
            'animal_study': 2,
            'in_vitro': 1,
            'commentary': 2,
            'news': 1,
            'unknown': 2,
        }
        level = type_levels.get(study_type, 2)
        
        # Adjust for source quality
        if source_quality == SourceQuality.HIGH:
            level = min(level + 1, 5)
        elif source_quality == SourceQuality.LOW:
            level = max(level - 1, 1)
        
        # Adjust for statistical rigor
        if rigor['has_effect_sizes'] and rigor['has_confidence_intervals']:
            level = min(level + 1, 5)
        
        # Adjust for sample size
        if sample_size:
            if sample_size >= 1000:
                level = min(level + 1, 5)
            elif sample_size < 50:
                level = max(level - 1, 1)
        
        return level


# Classifier
class ArticleClassifier:
    """Classify articles into project areas and sub-topics."""
    
    CLASSIFY_PROMPT = """You are a medical research classifier specializing in preventive health.

Classify this article into ONE of these project areas:
1. disease_prevention - Preventing diseases (heart, diabetes, cancer, neuro, mental, etc.)
2. behavioral_protocols - Exercise, sleep, meditation, risky behaviors
3. nutritional_protocols - Diet, supplements, hydration, macros/micros
4. government_interventions - Policy, pollution, food safety, public health
5. youth_health - School/college health programs, adolescent health

Article Title: {title}
Abstract: {abstract}

Respond in JSON format only:
{{
    "project_area": "<one of the 5 areas>",
    "sub_topic": "<specific sub-topic within the area>",
    "confidence": <0.0-1.0>,
    "keywords": ["<relevant keywords found>"]
}}"""
    
    def __init__(self, llm: LLMProvider):
        self.llm = llm
    
    async def classify(self, article: Article) -> LLMClassificationResult:
        """Classify article using LLM."""
        # First try keyword-based classification
        keyword_result = self._keyword_classify(article)
        
        if keyword_result.confidence >= 0.8:
            return keyword_result
        
        # Use LLM for uncertain cases
        try:
            prompt = self.CLASSIFY_PROMPT.format(
                title=article.title,
                abstract=truncate_text(article.abstract or "", 2000),
            )
            
            response = await self.llm.complete(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            
            # Parse JSON response
            result = json.loads(response)
            
            return LLMClassificationResult(
                project_area=ProjectArea(result.get("project_area", "general")),
                sub_topic=result.get("sub_topic", ""),
                confidence=result.get("confidence", 0.5),
                keywords=result.get("keywords", []),
            )
            
        except Exception as e:
            logger.error(f"LLM classification failed: {e}")
            return keyword_result
    
    def _keyword_classify(self, article: Article) -> LLMClassificationResult:
        """Fast keyword-based classification."""
        text = f"{article.title or ''} {article.abstract or ''}".lower()
        
        best_area = ProjectArea.GENERAL
        best_sub_topic = ""
        best_score = 0
        matched_keywords = []
        
        for area, sub_topics in PROJECT_KEYWORDS.items():
            for sub_topic, keywords in sub_topics.items():
                matches = [kw for kw in keywords if kw.lower() in text]
                score = len(matches)
                
                if score > best_score:
                    best_score = score
                    best_area = area
                    best_sub_topic = sub_topic
                    matched_keywords = matches
        
        # Calculate confidence based on matches
        confidence = min(best_score / 5, 1.0) if best_score > 0 else 0.1
        
        return LLMClassificationResult(
            project_area=best_area,
            sub_topic=best_sub_topic,
            confidence=confidence,
            keywords=matched_keywords[:10],
        )


# Summarizer
class ArticleSummarizer:
    """Generate summaries for preventive health articles."""
    
    SUMMARIZE_PROMPT = """You are a preventive health research summarizer.

Summarize this article focusing on:
1. Main findings relevant to disease prevention
2. Practical preventive implications
3. Quality of evidence

Article Title: {title}
Abstract: {abstract}
Study Type: {study_type}
Source Quality: {source_quality}

Respond in JSON format only:
{{
    "summary": "<2-3 sentence summary focusing on preventive health implications>",
    "key_findings": ["<finding 1>", "<finding 2>", "<finding 3>"],
    "preventive_implications": "<what this means for prevention>",
    "quality_assessment": "<brief assessment of evidence quality>",
    "relevance_score": <0-100 relevance to preventive health>
}}"""
    
    def __init__(self, llm: LLMProvider):
        self.llm = llm
    
    async def summarize(self, article: Article) -> LLMSummaryResult:
        """Summarize article using LLM."""
        try:
            prompt = self.SUMMARIZE_PROMPT.format(
                title=article.title,
                abstract=truncate_text(article.abstract or "", 3000),
                study_type=article.study_type or "unknown",
                source_quality=article.source_quality or "medium",
            )
            
            response = await self.llm.complete(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.3,
            )
            
            result = json.loads(response)
            
            return LLMSummaryResult(
                summary=result.get("summary", ""),
                key_findings=result.get("key_findings", []),
                preventive_implications=result.get("preventive_implications", ""),
                quality_assessment=result.get("quality_assessment", ""),
                relevance_score=result.get("relevance_score", 50),
            )
            
        except Exception as e:
            logger.error(f"LLM summarization failed: {e}")
            return LLMSummaryResult(
                summary=truncate_text(article.abstract or article.title, 300),
                key_findings=[],
                preventive_implications="Unable to assess",
                quality_assessment="Unable to assess",
                relevance_score=50,
            )


# Global instances
llm_provider = LLMProvider()
quality_filter = QualityFilter()
classifier = ArticleClassifier(llm_provider)
summarizer = ArticleSummarizer(llm_provider)


@app.post("/llm/filter", response_model=FilterResponse)
async def filter_article(request: FilterRequest):
    """Apply quality filter to article."""
    result = quality_filter.filter(request.article)
    
    return FilterResponse(
        article_id=request.article.article_id,
        passed=result.passed,
        source_quality=result.source_quality.value,
        study_type=result.study_type.value,
        evidence_level=result.evidence_level,
        reasons=result.reasons,
        warnings=result.warnings,
    )


@app.post("/llm/classify", response_model=ClassifyResponse)
async def classify_article(request: ClassifyRequest):
    """Classify article into project area."""
    result = await classifier.classify(request.article)
    
    return ClassifyResponse(
        article_id=request.article.article_id,
        project_area=result.project_area.value,
        sub_topic=result.sub_topic,
        confidence=result.confidence,
        keywords=result.keywords,
    )


@app.post("/llm/summarize", response_model=SummarizeResponse)
async def summarize_article(request: SummarizeRequest):
    """Summarize article."""
    result = await summarizer.summarize(request.article)
    
    return SummarizeResponse(
        article_id=request.article.article_id,
        summary=result.summary,
        key_findings=result.key_findings,
        preventive_implications=result.preventive_implications,
        quality_assessment=result.quality_assessment,
        relevance_score=result.relevance_score,
    )


@app.post("/llm/process-batch", response_model=BatchResponse)
async def process_batch(request: BatchRequest):
    """Process multiple articles: filter, classify, and summarize."""
    results = []
    passed_count = 0
    processed_count = 0
    
    for article in request.articles:
        # 1. Quality filter
        filter_result = quality_filter.filter(article)
        
        if not filter_result.passed:
            results.append({
                "article_id": article.article_id,
                "status": "filtered_out",
                "reasons": filter_result.reasons,
            })
            continue
        
        passed_count += 1
        
        # 2. Classify
        classify_result = await classifier.classify(article)
        
        # 3. Summarize
        summary_result = await summarizer.summarize(article)
        
        processed_count += 1
        
        results.append({
            "article_id": article.article_id,
            "status": "processed",
            "source_quality": filter_result.source_quality.value,
            "study_type": filter_result.study_type.value,
            "evidence_level": filter_result.evidence_level,
            "project_area": classify_result.project_area.value,
            "sub_topic": classify_result.sub_topic,
            "summary": summary_result.summary,
            "key_findings": summary_result.key_findings,
            "preventive_implications": summary_result.preventive_implications,
            "relevance_score": summary_result.relevance_score,
        })
    
    return BatchResponse(
        total=len(request.articles),
        passed_filter=passed_count,
        processed=processed_count,
        results=results,
    )


@app.get("/health")
async def health():
    """Health check."""
    return {
        "status": "healthy",
        "service": "llm",
        "provider": llm_provider.provider,
        "model": llm_provider.model,
    }