
def scan_zero_indent():
    with open("app.py", "r", encoding="utf-8") as f:
        lines = f.readlines()
        
    start = 876
    end = 2048
    
    for i in range(start, end):
        line = lines[i]
        if line.strip() and not line.startswith("    "):
            print(f"Line {i+1} has 0 indent: {line.strip()[:40]}...")

if __name__ == "__main__":
    scan_zero_indent()





