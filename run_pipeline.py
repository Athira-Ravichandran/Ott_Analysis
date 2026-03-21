"""
run_pipeline.py — Master script: runs entire OTT Pulse pipeline in sequence.

Usage:
    python run_pipeline.py
    python run_pipeline.py --skip-scrape   # use existing raw CSVs
    python run_pipeline.py --skip-mysql    # skip MySQL load
"""
import subprocess, sys, os, argparse, time
#from scraper_justwatch import scrape_platforms

def run_step(label, script, *args):
    print(f"\n{'='*60}\n  STEP: {label}\n{'='*60}")
    result = subprocess.run([sys.executable, script] + list(args),
                            cwd=os.path.dirname(__file__))
    if result.returncode != 0:
        print(f"\n  ERROR in: {label}"); sys.exit(1)
    print(f"  DONE: {label}"); time.sleep(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-mysql",  action="store_true")
    args = parser.parse_args()
    base = os.path.dirname(os.path.abspath(__file__))

    print("\n" + "="*60 + "\n  OTT PULSE — Full Pipeline\n" + "="*60)

    if not args.skip_scrape:
        run_step("Scrape IMDb", os.path.join(base,"scrapers","scraper_imdb.py"))
        run_step("Scrape JustWatch", os.path.join(base,"scrapers","scraper_justwatch.py"))
    else:
        print("\n  [Skipped] Scraping.")

    run_step("Clean & Transform", os.path.join(base,"clean_transform.py"))

    if not args.skip_mysql:
        run_step("Load to MySQL", os.path.join(base,"load_to_mysql.py"))
    else:
        print("\n  [Skipped] MySQL.")

    run_step("EDA Charts", os.path.join(base,"notebooks","eda_visualizations.py"))

    print("\n" + "="*60 + "\n  PIPELINE COMPLETE\n" + "="*60 + "\n")

if __name__ == "__main__":
    main()
