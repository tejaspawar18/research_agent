def classify_topics(summary: str):
    """
    Simple rule-based medical topic classifier.

    Extend later to ML-based.
    """

    if not summary:
        return []

    summary_l = summary.lower()

    topics = []

    if any(k in summary_l for k in ["cancer", "tumor", "oncology"]):
        topics.append("oncology")

    if any(k in summary_l for k in ["diabetes", "glucose", "insulin"]):
        topics.append("diabetes")

    if any(k in summary_l for k in ["covid", "influenza", "virus"]):
        topics.append("infectious-disease")

    if any(k in summary_l for k in ["machine learning", "deep learning", "model"]):
        topics.append("ai-ml")

    return topics
