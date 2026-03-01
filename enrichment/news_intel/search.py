"""
Layer 4: News & Public Record Intelligence
Searches for project/entity in news, court records, and municipal records.
"""
import os
import json
from datetime import datetime
from typing import Optional

import httpx
from loguru import logger

from openshovels.schema import NewsIntelligence, NewsHit


class NewsIntel:
    """
    Multi-source news and public record search.
    Surfaces deal context: zoning issues, community opposition,
    tax incentives, litigation, press coverage.
    """

    def __init__(
        self,
        google_news_api_key: Optional[str] = None,
        grok_api_key: Optional[str] = None,
    ):
        self.google_key = google_news_api_key or os.getenv("GOOGLE_NEWS_API_KEY")
        self.grok_key = grok_api_key or os.getenv("GROK_API_KEY")
        self.cost_tracker = 0.0

    async def search(
        self,
        entity_name: Optional[str] = None,
        address: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
    ) -> NewsIntelligence:
        """
        Search news and public records for project/entity context.
        """
        queries = self._build_queries(entity_name, address, city, state)
        all_articles: list[NewsHit] = []

        for query in queries:
            articles = await self._google_news_search(query)
            all_articles.extend(articles)

        # Deduplicate by URL
        seen_urls = set()
        unique_articles = []
        for article in all_articles:
            if article.url and article.url not in seen_urls:
                seen_urls.add(article.url)
                unique_articles.append(article)
            elif not article.url:
                unique_articles.append(article)

        # Build intelligence report
        intel = NewsIntelligence(
            search_query=" | ".join(queries),
            total_hits=len(unique_articles),
            articles=unique_articles[:20],  # Cap at 20
            search_timestamp=datetime.now(),
        )

        # Analyze for risk signals
        if unique_articles and self.grok_key:
            intel = await self._ai_analyze(intel)

        # Keyword-based flag detection (fast fallback)
        intel = self._detect_flags(intel)

        return intel

    async def _google_news_search(self, query: str) -> list[NewsHit]:
        """Search Google News via Custom Search API."""
        if not self.google_key:
            return await self._fallback_news_search(query)

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                # Google Custom Search JSON API
                resp = await client.get(
                    "https://www.googleapis.com/customsearch/v1",
                    params={
                        "key": self.google_key,
                        "cx": os.getenv("GOOGLE_CSE_ID", ""),
                        "q": query,
                        "num": 10,
                        "sort": "date",
                        "tbm": "nws",  # News search
                    },
                )

                self.cost_tracker += 0.005  # ~$5/1000 queries

                if resp.status_code != 200:
                    logger.debug(f"Google News API error: {resp.status_code}")
                    return []

                data = resp.json()
                items = data.get("items", [])

                return [
                    NewsHit(
                        title=item.get("title", ""),
                        source=item.get("displayLink", ""),
                        url=item.get("link", ""),
                        snippet=item.get("snippet", ""),
                    )
                    for item in items
                ]

        except Exception as e:
            logger.warning(f"Google News search failed: {e}")
            return []

    async def _fallback_news_search(self, query: str) -> list[NewsHit]:
        """Fallback: use Google search without API key."""
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                resp = await client.get(
                    "https://news.google.com/rss/search",
                    params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"},
                )

                if resp.status_code != 200:
                    return []

                # Parse RSS XML
                import xml.etree.ElementTree as ET
                root = ET.fromstring(resp.text)

                articles = []
                for item in root.findall(".//item")[:10]:
                    title = item.findtext("title", "")
                    link = item.findtext("link", "")
                    source = item.findtext("source", "")
                    pub_date = item.findtext("pubDate", "")
                    desc = item.findtext("description", "")

                    articles.append(NewsHit(
                        title=title,
                        source=source,
                        url=link,
                        snippet=desc[:300] if desc else "",
                    ))

                return articles

        except Exception as e:
            logger.debug(f"RSS news search failed: {e}")
            return []

    async def _ai_analyze(self, intel: NewsIntelligence) -> NewsIntelligence:
        """Use Grok to analyze news articles for risk signals."""
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=self.grok_key, base_url="https://api.x.ai/v1")

        articles_text = "\n".join([
            f"- {a.title} ({a.source}): {a.snippet or ''}"
            for a in intel.articles[:10]
        ])

        prompt = f"""Analyze these news articles about a real estate development project.
Identify any risk signals for a construction lender.

Articles:
{articles_text}

Return ONLY JSON:
{{
  "has_zoning_issues": true/false,
  "has_community_opposition": true/false,
  "has_tax_incentives": true/false,
  "has_litigation": true/false,
  "summary": "<2-3 sentence summary of key findings for a construction lender>",
  "sentiment_overall": "positive|negative|neutral|mixed"
}}"""

        try:
            response = await client.chat.completions.create(
                model="grok-3-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.1,
            )

            self.cost_tracker += 0.002

            text = response.choices[0].message.content.strip()
            text = text.replace("```json", "").replace("```", "").strip()
            result = json.loads(text)

            intel.has_zoning_issues = result.get("has_zoning_issues", False)
            intel.has_community_opposition = result.get("has_community_opposition", False)
            intel.has_tax_incentives = result.get("has_tax_incentives", False)
            intel.has_litigation = result.get("has_litigation", False)
            intel.ai_summary = result.get("summary", "")

        except Exception as e:
            logger.warning(f"AI news analysis failed: {e}")

        return intel

    def _detect_flags(self, intel: NewsIntelligence) -> NewsIntelligence:
        """Keyword-based flag detection from article titles/snippets."""
        all_text = " ".join([
            f"{a.title} {a.snippet or ''}" for a in intel.articles
        ]).lower()

        if not intel.has_zoning_issues:
            zoning_keywords = ["zoning", "variance", "zone change", "planning board",
                              "conditional use", "special permit", "overlay"]
            intel.has_zoning_issues = any(kw in all_text for kw in zoning_keywords)

        if not intel.has_community_opposition:
            opposition_keywords = ["opposition", "protest", "neighbors object",
                                  "community pushback", "residents oppose", "stop"]
            intel.has_community_opposition = any(kw in all_text for kw in opposition_keywords)

        if not intel.has_tax_incentives:
            incentive_keywords = ["tax break", "tax incentive", "tif", "tax increment",
                                 "abatement", "opportunity zone", "enterprise zone"]
            intel.has_tax_incentives = any(kw in all_text for kw in incentive_keywords)

        if not intel.has_litigation:
            lit_keywords = ["lawsuit", "sued", "litigation", "court", "injunction",
                           "lien", "foreclosure", "bankruptcy"]
            intel.has_litigation = any(kw in all_text for kw in lit_keywords)

        return intel

    def _build_queries(
        self,
        entity_name: Optional[str],
        address: Optional[str],
        city: Optional[str],
        state: Optional[str],
    ) -> list[str]:
        """Build search queries from available data."""
        queries = []
        location = f"{city} {state}" if city and state else ""

        if entity_name and location:
            queries.append(f'"{entity_name}" {location} construction development')

        if address and location:
            queries.append(f'"{address}" {location} building permit development')

        if entity_name:
            queries.append(f'"{entity_name}" real estate development')

        # Limit to 3 queries
        return queries[:3]
