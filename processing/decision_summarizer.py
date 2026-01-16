DECISION_SUMMARY_PROMPT = """
You are an expert research analyst.

Given the following research article abstract and metadata,
produce a DECISION-READY summary that helps a researcher decide
whether to read the full paper.

STRICT FORMAT (use markdown):

### 🧠 Research Question
- What specific problem does this paper address?

### 🧪 Methodology
- What methods, data, or experiments were used?

### 📊 Key Findings
- What are the main results or conclusions?

### 🌍 Why It Matters
- Why is this important or novel?

### ⚠️ Limitations / Caveats
- Any stated or obvious limitations (if none, say "Not stated").

### 👥 Who Should Read This
- Which researchers or practitioners would benefit most?

Use only the information provided.
Do NOT speculate.
Do NOT add references.
Keep total length under 250 words.
"""



from openai import OpenAI

client = OpenAI()

def generate_decision_summary(article):
    text = f"""
TITLE: {article.title}

JOURNAL: {article.journal}

ABSTRACT:
{article.abstract}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": DECISION_SUMMARY_PROMPT},
            {"role": "user", "content": text}
        ],
        temperature=0.2
    )

    return response.choices[0].message.content
