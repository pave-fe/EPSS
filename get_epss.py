import requests
import gzip
import csv

def download_and_unzip_gzip(url, output_file):
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(output_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    else:
        raise Exception(f"Failed to download the file from {url}. Status code: {response.status_code}")

def remove_first_row(input_file, output_file):
    with open(input_file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row

        with open(output_file, 'w', newline='') as output_csv:
            csv_writer = csv.writer(output_csv)
            for row in csv_reader:
                csv_writer.writerow(row)

if __name__ == "__main__":
    url = "https://epss.cyentia.com/epss_scores-current.csv.gz"
    gzip_file = "epss_scores-current.csv.gz"
    csv_file = "EPSS.csv"

    download_and_unzip_gzip(url, gzip_file)
    with gzip.open(gzip_file, "rt") as gz:
        with open(csv_file, "w") as f:
            f.write(gz.read())

    remove_first_row(csv_file, csv_file)

    print("Download, unzip, and CSV manipulation completed successfully.")
