import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "processor"))

from main import enrich_article, text_features  # noqa: E402


def test_text_features():
    result = text_features("Hello world. This is a test!")
    assert result["word_count"] == 6
    assert result["sentence_count"] == 2
    assert result["character_count"] == 28
    assert result["average_word_length"] > 0


def test_enrichment_preserves_source_fields():
    article = {
        "article_id": "a-1",
        "title": "Test article",
        "author": "Author",
        "publish_date": "2026-10-08T00:00:00",
        "content": "One two three.",
    }
    enriched = enrich_article(article, "123", "2026-10-08T00:00:00+00:00")
    assert enriched["article_id"] == article["article_id"]
    assert enriched["content"] == article["content"]
    assert enriched["features"]["word_count"] == 3
    assert enriched["pipeline_metadata"]["schema_version"] == 1
