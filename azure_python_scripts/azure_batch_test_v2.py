import os
import requests
import time
from datetime import datetime

# Azure credentials
endpoint = "https://doc-intel-test-instance.cognitiveservices.azure.com/"
key = "D5JxITW2yHCkMEe1MoWBML0eJlQUfP7XU4noRQRc3jAniqj5xKWTJQQJ99ALACYeBjFXJ3w3AAALACOGxKrB"

# API Endpoint
base_url = f"{endpoint}formrecognizer/documentModels/prebuilt-document:analyze?outputContentFormat=markdown&api-version=2023-07-31"

# Headers
headers = {
    "Ocp-Apim-Subscription-Key": key,
    "Content-Type": "application/pdf"
}

# Input and output directories
input_folder = "test_data/batch_10"  # Adjust the relative path for your Codespace
output_folder = "azure_output_markdowns"
report_folder = "azure_reports"
os.makedirs(output_folder, exist_ok=True)
os.makedirs(report_folder, exist_ok=True)

# Extract batch number dynamically from the input folder
batch_number = os.path.basename(input_folder)  # Extracts 'batch_10'

# Script version and test label
script_version = "v1.0"
test_label = "sequential_azure"

# Generate a unique Run ID
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Generate the report file path
report_file_path = os.path.join(
    report_folder, f"report_{script_version}_{test_label}_{batch_number}_{run_id}.txt"
)

# Create a subfolder for Markdown outputs for this specific run
run_output_folder = os.path.join(output_folder, f"{batch_number}_{run_id}")
os.makedirs(run_output_folder, exist_ok=True)

# Function to analyze a single document and save Markdown output
def process_document(file_path, output_path):
    start_time = time.time()  # Start timing the document processing
    try:
        print(f"Submitting {file_path} for analysis...")
        with open(file_path, "rb") as file:
            # Add `pages=all` parameter to process all pages
            response = requests.post(
                base_url,
                headers=headers,
                data=file,
                params={"pages": "1-1000"}  # Explicitly request all pages
            )

        if response.status_code == 202:
            # Poll for the result
            result_url = response.headers["operation-location"]
            while True:
                result_response = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": key})
                result_data = result_response.json()

                if result_response.status_code == 200 and result_data["status"] == "succeeded":
                    break
                elif result_data["status"] == "failed":
                    print("Page processing failed.")
                    return None, time.time() - start_time
                print(f"Processing {file_path}... Checking again in 5 seconds.")
                time.sleep(5)

            # Extract Markdown content
            markdown_content = result_data["analyzeResult"]["content"]

            # Save the Markdown content to the output folder
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            print(f"Markdown saved to: {output_path}")
            return True, time.time() - start_time  # Return success and processing time
        else:
            print(f"Error submitting {file_path}: {response.status_code}, {response.text}")
            return None, time.time() - start_time
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None, time.time() - start_time


# Start batch processing
batch_start_time = datetime.now()
total_docs = 0
successful_docs = 0
processing_times = []

# Open the report file
with open(report_file_path, "w", encoding="utf-8") as report_file:
    report_file.write(f"--- Batch Processing Report ({run_id}) ---\n")
    report_file.write(f"Script Version: {script_version}\n")
    report_file.write(f"Test Label: {test_label}\n")
    report_file.write(f"Batch Number: {batch_number}\n")
    report_file.write(f"Start Time: {batch_start_time}\n\n")

    # Loop through all PDF files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.lower().endswith(".pdf"):  # Process only PDF files
            total_docs += 1
            input_path = os.path.join(input_folder, file_name)
            output_file_name = os.path.splitext(file_name)[0] + ".md"
            output_path = os.path.join(run_output_folder, output_file_name)

            # Process each document and log processing time
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
    if processing_times:
        report_file.write(f"Average Processing Time Per Document: {sum(processing_times) / len(processing_times):.2f} seconds\n")
    report_file.write(f"Processing Time for Each Document: {processing_times}\n")
    report_file.write("---------------------------------\n")

print(f"Batch processing complete! Report saved to {report_file_path}. Markdown files saved in {run_output_folder}.")
