from processing.summarizer import summarize_pdf
import os

def test_summarizer():
    # Use existing test.pdf in the workspace if available, or create a dummy one?
    # Workspace has test.pdf (5002022 bytes).
    # If the file exists, we use it.
    file_path = "test.pdf"
    if not os.path.exists(file_path):
        print(f"File {file_path} not found. Cannot test.")
        return

    print(f"Testing summarizer with {file_path}...")
    title = "GTP release-selective agonists prolong opioid analgesic efficacy"
    doi = "https://doi.org/10.1038/s41586-025-09880-5"
    published_date = "2025-12-29"
    try:
        summary = summarize_pdf(file_path, title, doi, published_date)
        if summary:
            print("Successfully summarized PDF!")
            # print(summary) # Don't print huge JSON
            if "title" in summary:
                print(f"Title: {summary['title']}")
            if "input_token" in summary:
                print(f"Input Tokens: {summary['input_token']}")
            if "output_token" in summary:
                print(f"Output Tokens: {summary['output_token']}")
        else:
            print("Summarization returned None.")
    except Exception as e:
        print(f"Summarization crashed: {e}")

if __name__ == "__main__":
    test_summarizer()
