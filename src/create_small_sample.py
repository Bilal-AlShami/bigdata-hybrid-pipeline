import csv
import argparse
import os

def generate_sample(input_path, output_path, num_rows=100000):
    print("=== بدء استخراج العينة الصغيرة ===", flush=True)
    
    try:
        with open(input_path, 'r', encoding='utf-8', errors='ignore') as infile, \
             open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # كتابة صف العناوين (Header)
            header = next(reader)
            writer.writerow(header)
            
            count = 0
            for row in reader:
                writer.writerow(row)
                count += 1
                if count >= num_rows:
                    break
                    
        print(f"تم بنجاح! تم استخراج {count} صف وحفظها في: {output_path}", flush=True)
        return True
    except Exception as e:
        print(f"حدث خطأ أثناء استخراج العينة: {e}", flush=True)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a small reproducible sample from a huge CSV file.')
    parser.add_argument('--input', type=str, required=True, help='Input huge CSV file path')
    parser.add_argument('--rows', type=int, default=100000, help='Number of rows for the sample')
    parser.add_argument('--output', type=str, default='data/orders_small_sample.csv', help='Output sample CSV file path')
    
    args = parser.parse_args()
    
    # Ensure data dir exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    generate_sample(args.input, args.output, args.rows)