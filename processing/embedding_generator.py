import os
from openai import OpenAI
from core.logger import get_logger

logger = get_logger("embedding_generator")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def embed_text(text: str):
    """
    Generates embedding using text-embedding-3-small.
    """

    if not text:
        return None

    logger.info("Generating embeddings...")

    res = client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )

    return res.data[0].embedding
