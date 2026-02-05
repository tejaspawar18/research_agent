"""
LLM Service - AI-powered quality filtering, classification, and summarization.
"""
import logging
import os
import json
import re
import asyncio
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


def clean_json_response(response_text: str) -> str:
    """Clean LLM response for JSON parsing.

    Handles: markdown code blocks, preamble text, control characters.
    """
    text = response_text.strip()

    # Remove markdown code blocks
    if "```json" in text:
        text = text.split("```json", 1)[1]
    if "```" in text:
        # Take content before the closing ```
        parts = text.split("```")
        text = parts[0]
    text = text.strip()

    # Extract JSON object from surrounding text (LLMs often add preamble/postamble)
    # Find the first { and last } to extract the JSON object
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        text = text[first_brace:last_brace + 1]

    # Replace actual newlines/tabs with spaces (they're outside JSON strings)
    # But preserve escaped \n sequences (literal backslash-n in JSON strings)
    text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')

    # Remove other control characters
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)

    # Collapse multiple spaces
    text = re.sub(r' {2,}', ' ', text)

    return text


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
        max_tokens: int = 16000,
        temperature: float = 0.3,
    ) -> str:
        """Generate completion."""
        if self.provider == "anthropic":
            return await self._anthropic_complete(messages, max_tokens, temperature)
        elif self.provider == "gemini":
            return await self._gemini_complete(messages, max_tokens, temperature)
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

    async def _gemini_complete(self, messages, max_tokens, temperature) -> str:
        """Google Gemini API completion."""
        # Convert messages to Gemini format
        # Gemini uses "contents" with "parts"
        contents = []
        system_instruction = None

        for msg in messages:
            if msg["role"] == "system":
                system_instruction = msg["content"]
            elif msg["role"] == "user":
                contents.append({
                    "role": "user",
                    "parts": [{"text": msg["content"]}]
                })
            elif msg["role"] == "assistant":
                contents.append({
                    "role": "model",
                    "parts": [{"text": msg["content"]}]
                })

        # Use gemini-2.0-flash model
        model = self.model or "gemini-2.5-flash"
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"

        payload = {
            "contents": contents,
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": temperature,
                "thinkingConfig": {
                    "thinkingBudget": 1024,
                },
            }
        }

        if system_instruction:
            payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=90.0,
            )
            response.raise_for_status()
            data = response.json()

            # Extract text from Gemini response
            if "candidates" in data and data["candidates"]:
                candidate = data["candidates"][0]
                if "content" in candidate and "parts" in candidate["content"]:
                    return candidate["content"]["parts"][0]["text"]

            raise ValueError(f"Unexpected Gemini response format: {data}")


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

## CLASSIFICATION TASK
Determine if this article is relevant to preventive health research. If it is, classify it into ONE project area and ONE specific sub-topic. If it is NOT relevant, reject it.

CRITICAL: Only classify articles that are genuinely about human health, disease prevention, nutrition, exercise, public health policy, or medical research. Articles about technology (computing, engineering, physics, materials science, etc.) that merely mention health as a speculative future application are NOT relevant. Be strict.

## PROJECT AREAS AND SUB-TOPICS

**PROJECT 1: disease_prevention** - Preventing key diseases impacting physical, cognitive and emotional health
Sub-topics:
- "Preventing Atherosclerotic Heart Disease" - cardiovascular disease, atherosclerosis, cholesterol, lipids, statins, coronary artery disease
- "Preventing Type 2 Diabetes and Insulin Resistance" - insulin, glucose, beta cell, GLP-1, metformin, HbA1c, prediabetes
- "Preventing Other Key Metabolic Diseases" - hypertension, fatty liver, kidney disease, NAFLD, metabolic syndrome
- "Preventing Cancers" - carcinogenesis, tumor, immunotherapy, oncology, cancer screening, cancer prevention
- "Preventing Musculoskeletal Diseases" - osteoporosis, sarcopenia, bone density, joint health, orthopedic prevention
- "Preventing Neurodegenerative Diseases" - dementia, Alzheimer's, Parkinson's, neuroinflammation, cognitive decline prevention
- "Preventing Mental Health Conditions" - anxiety, depression, stress, mood disorders, psychological interventions
- "Maintaining Foundational Health" - immune system, gut microbiome, oral health, skin health, vision, hearing

**PROJECT 2: behavioral_protocols** - Behavioural protocols for improving healthspan
Sub-topics:
- "Using Cardiovascular Exercises" - aerobic training, endurance exercise, cardiac fitness
- "Using Resistance Training Exercises" - strength training, hypertrophy, resistance exercise
- "Using Stability and Mobility Exercises" - balance, flexibility, yoga, mobility training
- "Sleep" - circadian rhythm, sleep architecture, sleep quality, insomnia
- "Meditation, Breathwork and Related Practices" - meditation, mindfulness, breathing exercises
- "Emerging Behavioural Protocols" - cold exposure, heat therapy, cryotherapy, sauna, acupuncture
- "Risky Behaviours" - smoking, alcohol, substance abuse, addiction, vaping

**PROJECT 3: nutritional_protocols** - Nutritional protocols for improving healthspan
Sub-topics:
- "Protein" - amino acids, dietary protein, protein supplementation
- "Sugar and Carbohydrates" - glycemic index, carbohydrate metabolism, sugar intake
- "Fats and Oils" - dietary fat, omega fatty acids, saturated fat, trans fat
- "Hydration and Salts" - electrolytes, hydration, mineral balance
- "Comparison of Popular Diets and Dietary Techniques" - fasting, mediterranean diet, keto, vegan
- "Micronutrients and Conventional Supplements" - vitamins, minerals, supplementation
- "Emerging Supplements" - omega-3, magnesium, creatine, NAD, herbal supplements

**PROJECT 4: government_interventions** - Government interventions for preventive public health
Sub-topics:
- "Improving Nutritional Standards and Food Safety" - food labelling, food safety regulation
- "Preventing Respiratory Infections by Tackling Air Pollution" - air quality, PM2.5, COPD prevention
- "Preventing Gastrointestinal Infections by Tackling Water Pollution" - waterborne disease, water quality
- "Reducing Exposure to Key Toxins" - microplastics, heavy metals, environmental toxins
- "Driving Mass Behavioural Change Through Effective Public Health Communications" - health literacy, public health campaigns
- "Increasing Funding of Preventive Approaches to Public Health" - health policy funding, public health investment
- "Adapting Healthcare Professional Talent Base" - healthcare workforce, preventive care training

**PROJECT 5: youth_health** - Preparing youth for improved future healthspan
Sub-topics:
- "School and College Health Programs" - school health interventions, adolescent health, student wellness
- "Regional Youth Health Initiatives" - country-specific youth health programs

## ARTICLE TO CLASSIFY

Title: {title}
Abstract: {abstract}

## RESPONSE FORMAT (JSON only)

If the article IS relevant to preventive health:
{{
    "is_relevant": true,
    "project_area": "<one of: disease_prevention, behavioral_protocols, nutritional_protocols, government_interventions, youth_health>",
    "sub_topic": "<exact sub-topic name from list above>",
    "confidence": <0.0-1.0>,
    "keywords": ["<matched health keywords from article>"]
}}

If the article is NOT relevant to preventive health (e.g. technology, engineering, physics, materials science, pure chemistry, computer science, astronomy, etc.):
{{
    "is_relevant": false,
    "project_area": "general",
    "sub_topic": "",
    "confidence": 0.0,
    "keywords": []
}}"""
    
    def __init__(self, llm: LLMProvider):
        self.llm = llm
    
    async def classify(self, article: Article) -> LLMClassificationResult:
        """Classify article using LLM."""
        # First try keyword-based classification
        keyword_result = self._keyword_classify(article)

        # If keyword match is strong, use it directly
        if keyword_result.confidence >= 0.8:
            return keyword_result

        # Use LLM for all other cases (including low-keyword-match articles)
        try:
            prompt = self.CLASSIFY_PROMPT.format(
                title=article.title,
                abstract=truncate_text(article.abstract or "", 2000),
            )

            response = await self.llm.complete(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )

            # Clean and parse JSON response
            response_text = clean_json_response(response)
            result = json.loads(response_text)

            # If LLM says article is not relevant, return with general/low confidence
            if not result.get("is_relevant", True):
                logger.info(f"LLM marked article as not relevant: {article.title[:60]}")
                return LLMClassificationResult(
                    project_area=ProjectArea.GENERAL,
                    sub_topic="",
                    confidence=0.0,
                    keywords=[],
                )

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
        """Fast keyword-based classification with enhanced sub-topic matching."""
        text = f"{article.title or ''} {article.abstract or ''}".lower()

        best_area = ProjectArea.GENERAL
        best_sub_topic = ""
        best_score = 0
        matched_keywords = []

        for area, sub_topics in PROJECT_KEYWORDS.items():
            for sub_topic, keywords in sub_topics.items():
                matches = [kw for kw in keywords if kw.lower() in text]
                score = len(matches)

                # Boost score for multi-word phrase matches (more specific)
                for kw in keywords:
                    if len(kw.split()) > 1 and kw.lower() in text:
                        score += 2

                if score > best_score:
                    best_score = score
                    best_area = area
                    best_sub_topic = sub_topic
                    matched_keywords = matches

        # Require at least 2 keyword matches to classify via keywords alone
        # Single keyword matches are too unreliable (e.g. "brain" or "water")
        if best_score >= 5:
            confidence = 0.9
        elif best_score >= 3:
            confidence = 0.7
        elif best_score >= 2:
            confidence = 0.5
        else:
            # Too few matches - not confident enough, will fall through to LLM
            confidence = 0.2
            best_area = ProjectArea.GENERAL
            best_sub_topic = ""

        return LLMClassificationResult(
            project_area=best_area,
            sub_topic=best_sub_topic,
            confidence=confidence,
            keywords=matched_keywords[:10],
        )


# Summarizer
class ArticleSummarizer:
    """Generate summaries for preventive health articles."""
    
    SUMMARIZE_PROMPT = """You are a preventive health research summarizer. Your job is to read an article and produce a clear, informative summary that highlights what matters for disease prevention and healthspan.

## ARTICLE

Title: {title}
Study Type: {study_type}
Source Quality: {source_quality}

{content_section}

## INSTRUCTIONS

Write a summary with 5-7 bullet points covering:
- What the study investigated and why it matters for preventive health
- Key results and effect sizes (if reported)
- Practical preventive implications (what someone could do based on this)
- Limitations or caveats
- Quality of evidence

Do NOT repeat the article title, journal name, DOI, or publication date in the summary. Focus on the actual scientific content and findings.

## RESPONSE FORMAT (JSON only)

{{
    "summary": "- Bullet point 1\\n- Bullet point 2\\n- Bullet point 3\\n- Bullet point 4\\n- Bullet point 5",
    "key_findings": ["finding 1", "finding 2", "finding 3", "finding 4", "finding 5"],
    "preventive_implications": "<what this means for prevention in 1-2 sentences>",
    "quality_assessment": "<brief assessment of evidence quality>",
    "relevance_score": <0-100 relevance to preventive health>
}}

RULES:
- "summary" must be a string with 5-7 bullet points separated by \\n, each starting with "- "
- "key_findings" must be an array of 5-7 distinct findings
- Each bullet point should be 1-2 sentences of substantive content, not metadata
- "relevance_score" must be a number 0-100"""
    
    def __init__(self, llm: LLMProvider):
        self.llm = llm
    
    async def summarize(self, article: Article) -> LLMSummaryResult:
        """Summarize article using LLM."""
        try:
            # Use full_text if available (better summaries), otherwise abstract
            if article.full_text and len(article.full_text) > 200:
                content_section = f"Full Text:\n{truncate_text(article.full_text, 6000)}"
            elif article.abstract and len(article.abstract) > 50:
                content_section = f"Abstract:\n{truncate_text(article.abstract, 3000)}"
            else:
                content_section = f"Abstract:\n{article.abstract or 'Not available'}"

            prompt = self.SUMMARIZE_PROMPT.format(
                title=article.title,
                content_section=content_section,
                study_type=article.study_type or "unknown",
                source_quality=article.source_quality or "medium",
            )

            response = await self.llm.complete(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=16000,
                temperature=0.3,
            )

            # Clean and parse JSON response
            response_text = clean_json_response(response)
            try:
                result = json.loads(response_text)
            except json.JSONDecodeError as je:
                logger.error(f"Summarize JSON parse failed for '{article.title[:60]}': {je}")
                logger.error(f"Raw response (first 300): {response[:300]}")
                logger.error(f"Cleaned (first 300): {response_text[:300]}")
                raise

            # Handle summary - convert list to string if needed
            summary = result.get("summary", "")
            if isinstance(summary, list):
                # LLM returned a list of points, join them with newlines
                summary = "\n".join(f"- {point}" if not point.startswith("-") else point for point in summary)

            return LLMSummaryResult(
                summary=summary,
                key_findings=result.get("key_findings", []),
                preventive_implications=result.get("preventive_implications", ""),
                quality_assessment=result.get("quality_assessment", ""),
                relevance_score=result.get("relevance_score", 50),
            )

        except Exception as e:
            logger.error(f"LLM summarization failed for '{article.title[:60]}': {e}")
            # Do not fallback - let the article be retried in the next pipeline cycle
            raise


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

    for i, article in enumerate(request.articles):
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

        # Filter out articles that the LLM classified as not relevant to preventive health
        if classify_result.confidence == 0.0 or classify_result.project_area == ProjectArea.GENERAL:
            logger.info(f"Filtered out irrelevant article: {article.title[:60]}")
            results.append({
                "article_id": article.article_id,
                "status": "filtered_out",
                "reasons": ["Not relevant to preventive health"],
            })
            continue

        # Rate limit delay between LLM calls (avoid 429 errors)
        await asyncio.sleep(0.3)

        # 3. Summarize (if this fails, skip article - it will be retried next cycle)
        try:
            summary_result = await summarizer.summarize(article)
        except Exception as sum_err:
            logger.warning(f"Summarization failed, skipping for retry: {article.title[:60]} - {sum_err}")
            results.append({
                "article_id": article.article_id,
                "status": "skipped",
                "reasons": [f"Summarization failed: {str(sum_err)[:100]}"],
            })
            continue

        # Filter out articles with very low relevance score from summarization
        if summary_result.relevance_score < 30:
            logger.info(f"Filtered out low-relevance article (score={summary_result.relevance_score}): {article.title[:60]}")
            results.append({
                "article_id": article.article_id,
                "status": "filtered_out",
                "reasons": [f"Low relevance score: {summary_result.relevance_score}/100"],
            })
            continue

        # Rate limit delay between articles
        if i < len(request.articles) - 1:
            await asyncio.sleep(0.3)

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