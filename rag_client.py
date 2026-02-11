"""
RAG Client — query the vector DB with natural language.

Supports three interfaces:
  • Gradio web UI   (default: ``python rag_client.py``)
  • CLI one-shot    (``python rag_client.py "question"``)
  • CLI interactive  (``python rag_client.py --interactive``)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import List, Dict, Any, Optional

import gradio as gr

import embeddings
import qdrant_utils
import ingest_data as ingester
import llm as llm_module
from data_source import DirectoryWatcher, scan_directory

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Core query logic (shared by CLI and Gradio)
# ═══════════════════════════════════════════════════════════════════════════

def query(
    question: str,
    top_k: int | None = None,
    score_threshold: float | None = None,
    collection_name: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Embed the question and search the vector DB for relevant chunks.

    Returns a list of result dicts with: text, score, source_file, metadata.
    """
    vec = embeddings.embed_text(question)
    results = qdrant_utils.search(
        query_vector=vec,
        top_k=top_k,
        score_threshold=score_threshold,
        collection_name=collection_name,
    )
    return results


def format_results(results: List[Dict[str, Any]]) -> str:
    """Format search results for display."""
    if not results:
        return "No relevant results found."

    lines = []
    for i, r in enumerate(results, 1):
        lines.append(f"--- Result {i} (score: {r['score']:.4f}) ---")
        lines.append(f"Source: {r['source_file']}")
        lines.append(r["text"])
        lines.append("")
    return "\n".join(lines)


def ask(
    question: str,
    top_k: int | None = None,
    score_threshold: float | None = None,
    collection_name: str | None = None,
    provider_name: str | None = None,
    temperature: float = 0.3,
) -> Dict[str, Any]:
    """
    Full RAG pipeline: retrieve context chunks then ask the LLM.

    Returns a dict with: answer, sources (list of result dicts), provider, model.
    """
    results = query(question, top_k=top_k, score_threshold=score_threshold,
                    collection_name=collection_name)

    context_chunks = [r["text"] for r in results]

    provider = llm_module.get_provider(provider_name)
    answer = provider.rag_chat(
        question=question,
        context_chunks=context_chunks,
        temperature=temperature,
    )

    return {
        "answer": answer,
        "sources": results,
        "provider": provider.name,
        "model": provider.model,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Gradio UI
# ═══════════════════════════════════════════════════════════════════════════

# Background watcher state (shared across Gradio callbacks)
_watcher_thread: Optional[threading.Thread] = None
_watcher_instance: Optional[DirectoryWatcher] = None
_watcher_stats = {"files": 0, "chunks": 0, "running": False, "started_at": None}


def _gradio_search(question: str, top_k: int, threshold: float):
    """Gradio callback for the search tab."""
    if not question.strip():
        return "", []

    results = query(question, top_k=int(top_k), score_threshold=threshold)

    # Build a Markdown summary
    if not results:
        md = "### No relevant results found.\nTry lowering the score threshold or ingesting more documents."
        table_data = []
    else:
        md_parts = []
        table_data = []
        for i, r in enumerate(results, 1):
            source = Path(r["source_file"]).name if r["source_file"] else "—"
            score = r["score"]
            snippet = r["text"][:300] + ("…" if len(r["text"]) > 300 else "")
            md_parts.append(
                f"**Result {i}** — score `{score:.4f}` — *{source}*\n\n"
                f"> {snippet}\n"
            )
            table_data.append([i, f"{score:.4f}", source, r["text"][:200]])
        md = "\n---\n".join(md_parts)

    return md, table_data


def _gradio_ask(question: str, top_k: int, threshold: float, provider: str, temperature: float):
    """Gradio callback for the Ask AI tab."""
    if not question.strip():
        return "", ""

    try:
        provider_name = provider.lower().strip() if provider else None
        result = ask(
            question,
            top_k=int(top_k),
            score_threshold=threshold,
            provider_name=provider_name,
            temperature=temperature,
        )

        # Format the answer
        answer_md = (
            f"### Answer\n\n{result['answer']}\n\n"
            f"---\n*Provider: {result['provider']} | Model: {result['model']}*"
        )

        # Format the sources
        if result["sources"]:
            source_parts = []
            for i, s in enumerate(result["sources"], 1):
                name = Path(s["source_file"]).name if s["source_file"] else "—"
                snippet = s["text"][:200] + ("…" if len(s["text"]) > 200 else "")
                source_parts.append(
                    f"**{i}.** *{name}* (score: `{s['score']:.4f}`)\n> {snippet}"
                )
            sources_md = "### Retrieved context\n\n" + "\n\n".join(source_parts)
        else:
            sources_md = "*No context chunks were retrieved.*"

        return answer_md, sources_md

    except Exception as exc:
        logger.exception("Ask AI error")
        return f"❌ Error: {exc}", ""


def _gradio_ingest_file(file_obj):
    """Gradio callback: ingest an uploaded file."""
    if file_obj is None:
        return "⚠️ No file uploaded."

    # Gradio gives us a temp path; copy into content/ for persistence
    src = Path(file_obj.name if hasattr(file_obj, "name") else file_obj)
    dest = config.CONTENT_DIR / src.name
    dest.write_bytes(src.read_bytes())

    try:
        qdrant_utils.ensure_collection()
        count = ingester._stream_file(dest)
        info = qdrant_utils.collection_info()
        return (
            f"✅ **{dest.name}** ingested — **{count}** chunks created.\n\n"
            f"Collection now has **{info['points_count']}** points."
        )
    except Exception as exc:
        logger.exception("Ingest error")
        return f"❌ Error: {exc}"


def _gradio_ingest_scan():
    """Gradio callback: run a one-shot scan of content/."""
    try:
        summary = ingester.ingest_scan()
        return (
            f"✅ Scan complete.\n\n"
            f"- Files ingested: **{summary['files_ingested']}**\n"
            f"- Chunks created: **{summary['chunks_created']}**\n"
            f"- Time: **{summary['elapsed_seconds']}s**"
        )
    except Exception as exc:
        logger.exception("Scan error")
        return f"❌ Error: {exc}"


def _watcher_loop(scan_existing: bool = True):
    """
    Background thread that continuously watches content/ for new files
    and streams them through the ingestion pipeline.

    This runs for the entire lifetime of the application.
    """
    global _watcher_instance
    _watcher_instance = DirectoryWatcher(
        directory=config.CONTENT_DIR, scan_existing=scan_existing
    )
    _watcher_instance.start()
    _watcher_stats["running"] = True
    _watcher_stats["started_at"] = time.time()

    logger.info("🔄 Background watcher active — monitoring %s", config.CONTENT_DIR)

    try:
        for fe in _watcher_instance:
            if not _watcher_stats["running"]:
                break
            try:
                count = ingester._stream_file(fe.path)
                _watcher_stats["files"] += 1
                _watcher_stats["chunks"] += count
                logger.info(
                    "📥 Auto-ingested %s — %d chunks (total files: %d, chunks: %d)",
                    fe.path.name, count,
                    _watcher_stats["files"], _watcher_stats["chunks"],
                )
            except Exception:
                logger.exception("Watcher ingest error for %s", fe.path.name)
    finally:
        _watcher_stats["running"] = False
        if _watcher_instance:
            _watcher_instance.stop()
            _watcher_instance = None


def start_background_watcher(scan_existing: bool = True) -> None:
    """
    Start the directory watcher as a daemon thread.

    Called automatically when the RAG client starts. The watcher runs
    for the entire lifetime of the process — any file dropped into
    ``content/`` is automatically chunked, embedded, and ingested.
    """
    global _watcher_thread
    if _watcher_stats["running"]:
        logger.info("Watcher already running — skipping duplicate start.")
        return

    qdrant_utils.ensure_collection()
    _watcher_stats.update(files=0, chunks=0, running=True, started_at=time.time())
    _watcher_thread = threading.Thread(
        target=_watcher_loop,
        kwargs={"scan_existing": scan_existing},
        daemon=True,
        name="content-watcher",
    )
    _watcher_thread.start()
    logger.info("✅ Background content watcher started (scan_existing=%s)", scan_existing)


def _gradio_watcher_status():
    """Gradio callback: show live watcher stats."""
    if not _watcher_stats["running"]:
        return "🔴 **Watcher is not running.**"

    uptime = time.time() - (_watcher_stats["started_at"] or time.time())
    mins, secs = divmod(int(uptime), 60)
    hrs, mins = divmod(mins, 60)
    uptime_str = f"{hrs}h {mins}m {secs}s" if hrs else f"{mins}m {secs}s"

    return (
        f"🟢 **Watcher is running** — uptime: {uptime_str}\n\n"
        f"- Files auto-ingested: **{_watcher_stats['files']}**\n"
        f"- Chunks created: **{_watcher_stats['chunks']}**\n\n"
        f"Drop files into `content/` — they will be ingested automatically."
    )


def _gradio_collection_status():
    """Gradio callback: get Qdrant collection status."""
    try:
        info = qdrant_utils.collection_info()
        files_in_content = list(scan_directory(config.CONTENT_DIR))
        watcher_status = "🟢 Running" if _watcher_stats["running"] else "🔴 Stopped"
        return (
            f"### Collection: `{info['name']}`\n\n"
            f"| Metric | Value |\n|---|---|\n"
            f"| Points in DB | **{info['points_count']}** |\n"
            f"| Vector dimension | **{info['vector_size']}** |\n"
            f"| Collection status | **{info['status']}** |\n"
            f"| Files in content/ | **{len(files_in_content)}** |\n"
            f"| Watcher | {watcher_status} |\n"
            f"| Watcher session files | {_watcher_stats['files']} |\n"
            f"| Watcher session chunks | {_watcher_stats['chunks']} |\n"
        )
    except Exception as exc:
        return f"⚠️ Could not fetch status: {exc}\n\nMake sure the collection is created (`make schema-create`)."


def build_gradio_app() -> gr.Blocks:
    """Construct and return the Gradio Blocks app."""

    with gr.Blocks(
        title="Dynamic RAG",
        theme=gr.themes.Soft(),
    ) as app:
        gr.Markdown("# 🔍 Dynamic Embedding RAG\nQuery your vector database, ask AI, or ingest new documents.")

        # ── Ask AI tab ────────────────────────────────────────────────
        with gr.Tab("🤖 Ask AI"):
            with gr.Row():
                with gr.Column(scale=3):
                    ask_input = gr.Textbox(
                        label="Question",
                        placeholder="Ask a question — the LLM will answer using your ingested documents…",
                        lines=3,
                    )
                with gr.Column(scale=1):
                    ask_provider = gr.Dropdown(
                        choices=llm_module.list_providers(),
                        value=config.LLM_PROVIDER,
                        label="LLM Provider",
                    )
                    ask_top_k = gr.Slider(
                        minimum=1, maximum=20, value=config.TOP_K_RESULTS,
                        step=1, label="Context chunks (Top-K)",
                    )
                    ask_threshold = gr.Slider(
                        minimum=0.0, maximum=1.0, value=config.SEARCH_THRESHOLD,
                        step=0.05, label="Score threshold",
                    )
                    ask_temp = gr.Slider(
                        minimum=0.0, maximum=1.5, value=0.3,
                        step=0.1, label="Temperature",
                    )
            ask_btn = gr.Button("Ask", variant="primary")

            ask_answer_md = gr.Markdown(label="Answer")
            ask_sources_md = gr.Markdown(label="Sources")

            ask_btn.click(
                fn=_gradio_ask,
                inputs=[ask_input, ask_top_k, ask_threshold, ask_provider, ask_temp],
                outputs=[ask_answer_md, ask_sources_md],
            )
            ask_input.submit(
                fn=_gradio_ask,
                inputs=[ask_input, ask_top_k, ask_threshold, ask_provider, ask_temp],
                outputs=[ask_answer_md, ask_sources_md],
            )

        # ── Search tab ────────────────────────────────────────────────
        with gr.Tab("🔎 Search"):
            with gr.Row():
                with gr.Column(scale=3):
                    q_input = gr.Textbox(
                        label="Question",
                        placeholder="Ask something…",
                        lines=2,
                    )
                with gr.Column(scale=1):
                    top_k_slider = gr.Slider(
                        minimum=1, maximum=20, value=config.TOP_K_RESULTS,
                        step=1, label="Top-K",
                    )
                    threshold_slider = gr.Slider(
                        minimum=0.0, maximum=1.0, value=config.SEARCH_THRESHOLD,
                        step=0.05, label="Score threshold",
                    )
            search_btn = gr.Button("Search", variant="primary")

            results_md = gr.Markdown(label="Results")
            results_table = gr.Dataframe(
                headers=["#", "Score", "Source", "Snippet"],
                label="Results table",
                interactive=False,
            )

            search_btn.click(
                fn=_gradio_search,
                inputs=[q_input, top_k_slider, threshold_slider],
                outputs=[results_md, results_table],
            )
            q_input.submit(
                fn=_gradio_search,
                inputs=[q_input, top_k_slider, threshold_slider],
                outputs=[results_md, results_table],
            )

        # ── Ingest tab ────────────────────────────────────────────────
        with gr.Tab("📥 Ingest"):
            gr.Markdown(
                "Upload a file or scan the `content/` directory to ingest documents into Qdrant.\n\n"
                "The **background watcher** is always active — any file dropped into "
                "`content/` is automatically chunked, embedded, and ingested."
            )

            with gr.Row():
                with gr.Column():
                    upload = gr.File(label="Upload a document", file_types=[".txt", ".md", ".html", ".json", ".csv"])
                    upload_btn = gr.Button("Ingest uploaded file", variant="primary")
                    upload_result = gr.Markdown()

                with gr.Column():
                    scan_btn = gr.Button("🔄 Re-scan content/ directory", variant="secondary")
                    scan_result = gr.Markdown()

            gr.Markdown("---")
            gr.Markdown("### 🔄 Live watcher status")
            watcher_status_md = gr.Markdown()
            watcher_refresh_btn = gr.Button("Refresh watcher status", variant="secondary")

            upload_btn.click(fn=_gradio_ingest_file, inputs=[upload], outputs=[upload_result])
            scan_btn.click(fn=_gradio_ingest_scan, outputs=[scan_result])
            watcher_refresh_btn.click(fn=_gradio_watcher_status, outputs=[watcher_status_md])
            app.load(fn=_gradio_watcher_status, outputs=[watcher_status_md])

        # ── Status tab ────────────────────────────────────────────────
        with gr.Tab("📊 Status"):
            refresh_btn = gr.Button("Refresh", variant="secondary")
            status_md = gr.Markdown()
            refresh_btn.click(fn=_gradio_collection_status, outputs=[status_md])
            # Auto-load on tab visit
            app.load(fn=_gradio_collection_status, outputs=[status_md])

    return app


# ═══════════════════════════════════════════════════════════════════════════
# CLI mode
# ═══════════════════════════════════════════════════════════════════════════

def interactive_mode(collection_name: str | None = None) -> None:
    """Run an interactive query loop."""
    print("\n🔍  RAG Interactive Query")
    print("    Type your question and press Enter. Type 'quit' to exit.\n")

    while True:
        try:
            q = input("Question> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not q or q.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break

        results = query(q, collection_name=collection_name)
        print(format_results(results))


def main() -> None:
    parser = argparse.ArgumentParser(description="RAG Client")
    parser.add_argument("question", nargs="?", default=None, help="Question to search")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive CLI mode")
    parser.add_argument("--no-ui", action="store_true", help="Disable Gradio UI (CLI only)")
    parser.add_argument("--ask", action="store_true", help="Use LLM to answer (RAG mode)")
    parser.add_argument("--provider", type=str, default=None, help="LLM provider: ollama, openai, ghcp")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--collection", type=str, default=None)
    parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    if args.interactive:
        # Start watcher in background even for CLI interactive mode
        start_background_watcher(scan_existing=True)
        interactive_mode(collection_name=args.collection)
    elif args.question:
        if args.ask:
            result = ask(
                args.question,
                top_k=args.top_k,
                collection_name=args.collection,
                provider_name=args.provider,
            )
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                print(f"\n{result['answer']}\n")
                print(f"--- {result['provider']} / {result['model']} ---")
                print(f"Sources: {len(result['sources'])} chunks retrieved")
        else:
            results = query(args.question, top_k=args.top_k, collection_name=args.collection)
            if args.json:
                print(json.dumps(results, indent=2))
            else:
                print(format_results(results))
    else:
        # Start the background content watcher before launching the UI.
        # It will scan existing files in content/ and then continuously
        # watch for new/modified files for the lifetime of the process.
        start_background_watcher(scan_existing=True)

        # Launch Gradio UI
        app = build_gradio_app()
        app.launch(
            server_name=config.GRADIO_SERVER_NAME,
            server_port=config.GRADIO_SERVER_PORT,
            share=config.GRADIO_SHARE,
        )


if __name__ == "__main__":
    main()
