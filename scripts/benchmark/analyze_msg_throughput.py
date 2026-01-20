#!/usr/bin/env python3
import sys
import re
import os

def analyze_log(log_path, threshold_mb_s):
    """
    Parses the benchmark log for 'Avg MB/s: X MB/s' and asserts X >= threshold.
    """
    if not os.path.exists(log_path):
        print(f"ERROR: Log file not found: {log_path}")
        return False, 0.0

    avg_mb_s = 0.0
    found = False
    
    # Regex to capture: "Avg MB/s: 6.57 MB/s"
    # The Scala benchmark prints: System.err.println(f"Avg MB/s: $avgMbS%.2f MB/s")
    pattern = re.compile(r"Avg MB/s:\s*([\d\.]+)\s*MB/s")

    with open(log_path, 'r') as f:
        for line in f:
            match = pattern.search(line)
            if match:
                avg_mb_s = float(match.group(1))
                found = True
                # We keep reading to get the *final* one (in case of intermediate reports)
                # The final block usually has "Benchmark Final Result" header, 
                # but the line format is same. Intermediate uses "Avg Throughput: ... (X MB/s)" vs Final "Avg MB/s: X"
                # Actually, BenchmarkListener prints "Avg Throughput: ... (X MB/s)".
                # The Main app prints "Avg MB/s: X MB/s" at the very end.
                # Let's target the Main app's final print which is specifically "Avg MB/s: ...".
    
    if not found:
        print(f"WARNING: No throughput metric found in {log_path}. Job might have failed or not finished.")
        return False, 0.0

    print(f"Detected Throughput: {avg_mb_s:.2f} MB/s (Threshold: {threshold_mb_s} MB/s)")
    
    if avg_mb_s >= threshold_mb_s:
        return True, avg_mb_s
    else:
        return False, avg_mb_s

def main():
    if len(sys.argv) < 3:
        print("Usage: analyze_msg_throughput.py <log_file> <threshold_mb_s>")
        sys.exit(1)

    log_file = sys.argv[1]
    threshold = float(sys.argv[2])

    success, value = analyze_log(log_file, threshold)
    
    # Output for shell script capture
    print(f"RESULT_MB_S={value}")
    
    if success:
        print("STATUS=PASS")
        sys.exit(0)
    else:
        print("STATUS=FAIL")
        sys.exit(1)

if __name__ == "__main__":
    main()
