# mdm-pipeline-5

Minimal instructions to run the MDM pipeline script (no emojis).

Prerequisites
- Python 3.10+ (or 3.9+ should work)
- Git (optional)

Quick setup
1. (Optional) create and activate a virtual environment:

	 python -m venv .venv
	 source .venv/bin/activate

2. Install required Python packages:

	 pip install httpx pandas tqdm python-dotenv openai rapidfuzz langdetect deep-translator openpyxl

Environment
Create a `.env` file in the project root with the following variables set:

```
OPENAI_API_KEY=your_openai_api_key_here
AZURE_MAPS_KEY=your_azure_maps_key_here
# Optional: override input/output file names (or edit constants in the script)
# INPUT_CSV=100_sample_MDM.csv
# OUTPUT_CSV=mdm_pipeline_results.csv
```

Running
- Run the full pipeline:

	python mdm_pipeline_script.py

- Run in test mode (first few rows):

	python mdm_pipeline_script.py --test

- Run with a custom input file:

	python mdm_pipeline_script.py --input my_data.csv

Notes
- The script expects `OPENAI_API_KEY` and `AZURE_MAPS_KEY` in the environment and will raise an error if they are missing.
- Default input/output file names are defined inside `mdm_pipeline_script.py` (see `INPUT_CSV` / `OUTPUT_CSV`). Edit those constants or pass a custom input file via `--input`.

If you want, I can add a `requirements.txt` and a `.gitignore` next.
