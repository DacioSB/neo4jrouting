import json
import sys
from tqdm import tqdm

def filter_geojson_by_city(input_file_path, output_file_path, target_city):
    """
    Reads a newline-delimited GeoJSON file and writes features matching
    the target city to a new file.
    """
    print(f"Filtering addresses for city: '{target_city}'...")
    
    total_lines = 0
    with open(input_file_path, 'r', encoding='utf-8') as f:
        for _ in f:
            total_lines += 1

    found_count = 0
    with open(input_file_path, 'r', encoding='utf-8') as infile, \
         open(output_file_path, 'w', encoding='utf-8') as outfile:
        
        for line in tqdm(infile, total=total_lines, desc="Processing addresses"):
            try:
                feature = json.loads(line)
                if feature.get("properties", {}).get("city") == target_city:
                    outfile.write(line)
                    found_count += 1
            except json.JSONDecodeError:
                print(f"Warning: Could not decode line: {line.strip()}")

    print(f"Done. Found and saved {found_count} addresses to '{output_file_path}'.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python filter_addresses.py <input_file.geojsonl> <output_file.geojsonl> \"<Target City Name>\"")
        sys.exit(1)
        
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    city_name = sys.argv[3]
    
    filter_geojson_by_city(input_file, output_file, city_name)