import os
from docling.document_converter import DocumentConverter
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Input and output directories
input_folder = "test_data/batch_10"  # Specify the current batch folder
output_folder = "docling_output_markdowns"
report_folder = "docling_reports"
os.makedirs(output_folder, exist_ok=True)
os.makedirs(report_folder, exist_ok=True)

# Extract batch number dynamically from the input folder
batch_number = os.path.basename(input_folder)  # Extracts 'batch_10'

# Script version and test label
script_version = "v2.0"
test_label = "parallel_test"

# Generate a unique Run ID
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Generate the report file path
report_file_path = os.path.join(
    report_folder, f"report_{script_version}_{test_label}_{batch_number}_{run_id}.txt"
)

# Create a subfolder for Markdown outputs for this specific run
run_output_folder = os.path.join(output_folder, f"{batch_number}_{run_id}")
os.makedirs(run_output_folder, exist_ok=True)

# Initialize the Docling converter
converter = DocumentConverter()

# Function to process a single document
def process_document(file_name):
    input_path = os.path.join(input_folder, file_name)
    output_file_name = os.path.splitext(file_name)[0] + ".md"
    output_path = os.path.join(run_output_folder, output_file_name)

    start_time = datetime.now()
    print(f"Processing: {input_path}...")
    try:
        # Convert the document
        result = converter.convert(input_path)
        markdown_output = result.document.export_to_markdown()

        # Save the Markdown output
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(markdown_output)
        duration = (datetime.now() - start_time).total_seconds()
        print(f"Saved Markdown to: {output_path} | Time: {duration:.2f} seconds")
        return {"file_name": file_name, "success": True, "duration": duration}
    except Exception as e:
        print(f"Error processing {input_path}: {e}")
        duration = (datetime.now() - start_time).total_seconds()
        return {"file_name": file_name, "success": False, "duration": duration, "error": str(e)}

# Start batch processing with parallel execution
batch_start_time = datetime.now()
files_to_process = [file_name for file_name in os.listdir(input_folder) if file_name.lower().endswith(".pdf")]

# Use ThreadPoolExecutor for parallel processing
results = []
with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers based on your environment
    futures = {executor.submit(process_document, file_name): file_name for file_name in files_to_process}

    for future in as_completed(futures):
        results.append(future.result())

# Summarize operational data
total_docs = len(files_to_process)
successful_docs = sum(1 for result in results if result["success"])
processing_times = [result["duration"] for result in results if result["success"]]
total_runtime = (datetime.now() - batch_start_time).total_seconds()

# Save the batch processing report
with open(report_file_path, "w", encoding="utf-8") as report_file:
    report_file.write(f"--- Batch Processing Report ({run_id}) ---\n")
    report_file.write(f"Script Version: {script_version}\n")
    report_file.write(f"Test Label: {test_label}\n")
    report_file.write(f"Batch Number: {batch_number}\n")
    report_file.write(f"Start Time: {batch_start_time}\n\n")
    for result in results:
        if result["success"]:
            report_file.write(f"Processed: {result['file_name']} | Time: {result['duration']:.2f} seconds\n")
        else:
            report_file.write(f"Failed: {result['file_name']} | Time: {result['duration']:.2f} seconds | Error: {result['error']}\n")
    report_file.write("\n--- Summary ---\n")
    report_file.write(f"Total Documents Analyzed: {total_docs}\n")
    report_file.write(f"Successful Documents: {successful_docs}\n")
    report_file.write(f"Total Time to Complete: {total_runtime:.2f} seconds\n")
    if processing_times:
        report_file.write(f"Average Processing Time Per Document: {sum(processing_times) / len(processing_times):.2f} seconds\n")
    report_file.write(f"Processing Time for Each Document: {processing_times}\n")
    report_file.write("---------------------------------\n")

print(f"Batch processing complete! Report saved to {report_file_path}. Markdown files saved in {run_output_folder}.")
