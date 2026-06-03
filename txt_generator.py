import os
import random

def generate_text_files():
    # Target sizes in bytes
    sizes = {
        "1MB": 1024 * 1024,
        "100MB": 100 * 1024 * 1024,
        "1GB": 1024 * 1024 * 1024,
        "5GB": 5 * 1024 * 1024 * 1024
    }

    # Common English words to fill the files
    words = [
        "the", "be", "to", "of", "and", "a", "in", "that", "have", "it", 
        "for", "not", "on", "with", "he", "as", "you", "do", "at", "this",
        "but", "his", "by", "from", "they", "we", "say", "her", "she", "or"
    ]
    
    # Pre-generate a line buffer to speed up I/O
    line_buffer = " ".join(random.choices(words, k=20)) + "\n"
    buffer_bytes = line_buffer.encode('utf-8')
    
    for label, target_bytes in sizes.items():
        filename = f"dataset_{label}.txt"
        print(f"[*] Generating {filename}...")
        
        bytes_written = 0
        with open(filename, "wb") as f:
            while bytes_written < target_bytes:
                f.write(buffer_bytes)
                bytes_written += len(buffer_bytes)
                
        print(f"  -> Done. Final size: {os.path.getsize(filename)} bytes")

if __name__ == "__main__":
    generate_text_files()