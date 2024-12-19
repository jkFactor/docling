import os
from docling.document_converter import DocumentConverter
from datetime import datetime

# Use the online GitHub repository for test data
input_folder = "test_data/batch_10"  # Relative path to the cloned test data folder in your repo
output_folder = "docling_output_markdowns"  # Base folder to save the Markdown outputs
report_folder = "docling_reports"  # Folder to save the batch processing reports
os.makedirs(output_folder, exist_ok=True)  # Ensure the base output folder exists
os.makedirs(report_folder, exist_ok=True)  # Ensure the report folder exists

# Initialize the Docling converter
converter = DocumentConverter()

# Generate a unique Run ID for the current run
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
run_output_folder = os.path.join(output_folder, run_id)  # Create a subfolder for this run
os.makedirs(run_output_folder, exist_ok=True)  # Ensure the subfolder exists
report_file_path = os.path.join(report_folder, f"report_{run_id}.txt")

# Function to process a single document
def process_document(input_path, output_path):
    start_time = datetime.now()
    print(f"Processing: {input_path}...")
    try:
        # Convert the document
        result = converter.convert(input_path)
        markdown_output = result.document.export_to_markdown()

        # Save the Markdown output
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(markdown_output)
        print(f"Saved Markdown to: {output_path}")
        duration = (datetime.now() - start_time).total_seconds()
        return True, duration  # Success, Processing time
    except Exception as e:
        print(f"Error processing {input_path}: {e}")
        duration = (datetime.now() - start_time).total_seconds()
        return False, duration  # Failure, Processing time

# Start batch processing
batch_start_time = datetime.now()
total_docs = 0
successful_docs = 0
processing_times = []

# Open the report file
with open(report_file_path, "w", encoding="utf-8") as report_file:
    report_file.write(f"--- Batch Processing Report ({run_id}) ---\n")
    report_file.write(f"Start Time: {batch_start_time}\n\n")

    # Loop through the documents in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.lower().endswith(".pdf"):  # Case-insensitive check for PDFs
            total_docs += 1
            input_path = os.path.join(input_folder, file_name)
            output_file_name = os.path.splitext(file_name)[0] + ".md"
            output_path = os.path.join(run_output_folder, output_file_name)  # Save in run-specific subfolder

            # Process the document
            success, duration = process_document(input_path, output_path)
            if success:
                successful_docs += 1
                processing_times.append(duration)
                report_file.write(f"Processed: {file_name} | Time: {duration:.2f} seconds\n")
            else:
                report_file.write(f"Failed: {file_name} | Time: {duration:.2f} seconds\n")

    # Summarize operational data
    total_runtime = (datetime.now() - batch_start_time).total_seconds()
    report_file.write("\n--- Summary ---\n")
    report_file.write(f"Total Documents Analyzed: {total_docs}\n")
    report_file.write(f"Successful Documents: {successful_docs}\n")
    report_file.write(f"Total Time to Complete: {total_runtime:.2f} seconds\n")
    report_file.write(f"Average Processing Time Per Document: {sum(processing_times) / len(processing_times):.2f} seconds\n" if processing_times else "No documents processed successfully.\n")
    report_file.write(f"Processing Time for Each Document: {processing_times}\n")
    report_file.write("---------------------------------\n")

print(f"Batch processing complete! Report saved to {report_file_path}. Markdown files saved in {run_output_folder}.")
