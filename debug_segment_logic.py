"""
Debug the segment rotation and overlap logic for Avigel's case
"""

MINUTES_PER_DAY = 1440

def span_minutes(start_time, end_time):
    """Convert HH:MM times to minutes since midnight"""
    def time_to_min(t):
        h, m = map(int, t.split(':'))
        return h * 60 + m

    start = time_to_min(start_time)
    end = time_to_min(end_time)

    if end < start:
        end += MINUTES_PER_DAY

    return start, end

def overlap_minutes(a_start, a_end, b_start, b_end):
    """Calculate overlap between two time ranges"""
    return max(0, min(a_end, b_end) - max(a_start, b_start))

# Simulate Avigel's case
print("="*80)
print("SIMULATING AVIGEL'S CASE: 15:00-08:00")
print("="*80 + "\n")

# Report times
report_start = "15:00"
report_end = "08:00"
rep_start_orig, rep_end_orig = span_minutes(report_start, report_end)

print(f"Report: {report_start} - {report_end}")
print(f"  rep_start_orig: {rep_start_orig} minutes ({rep_start_orig/60:.2f} hours)")
print(f"  rep_end_orig: {rep_end_orig} minutes ({rep_end_orig/60:.2f} hours)")

# Shift segments
segments = [
    {"start_time": "16:00", "end_time": "22:00", "segment_type": "work", "wage_percent": 100},
    {"start_time": "22:00", "end_time": "06:30", "segment_type": "standby", "wage_percent": 24},
    {"start_time": "06:30", "end_time": "08:00", "segment_type": "work", "wage_percent": 100},
]

print(f"\nShift Segments:")
for i, seg in enumerate(segments):
    s_start, s_end = span_minutes(seg["start_time"], seg["end_time"])
    print(f"  {i}: {seg['start_time']}-{seg['end_time']} ({seg['segment_type']}, {seg['wage_percent']}%)")
    print(f"     start_min: {s_start}, end_min: {s_end}")

# Sort segments
seg_list_sorted = sorted(segments, key=lambda s: span_minutes(s["start_time"], s["end_time"])[0])
print(f"\nSorted segments:")
for i, seg in enumerate(seg_list_sorted):
    print(f"  {i}: {seg['start_time']}-{seg['end_time']}")

# Find rotation index
rep_start_min = rep_start_orig % MINUTES_PER_DAY
print(f"\nrep_start_min: {rep_start_min} ({rep_start_min/60:.2f} hours)")

best_start_diff = -1
rotate_idx = 0

for i, seg in enumerate(seg_list_sorted):
    seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])
    print(f"\nChecking segment {i}: {seg['start_time']}")
    print(f"  seg_start_min: {seg_start_min}")
    print(f"  seg_start_min <= rep_start_min? {seg_start_min} <= {rep_start_min} = {seg_start_min <= rep_start_min}")

    if seg_start_min <= rep_start_min:
        if seg_start_min > best_start_diff:
            best_start_diff = seg_start_min
            rotate_idx = i
            print(f"  → Updated: best_start_diff={best_start_diff}, rotate_idx={rotate_idx}")

print(f"\nAfter loop: best_start_diff={best_start_diff}, rotate_idx={rotate_idx}")

if best_start_diff == -1 and seg_list_sorted:
    first_seg_start, _ = span_minutes(seg_list_sorted[0]["start_time"], seg_list_sorted[0]["end_time"])
    last_seg_start, last_seg_end = span_minutes(seg_list_sorted[-1]["start_time"], seg_list_sorted[-1]["end_time"])

    print(f"\nNo segment found before report start!")
    print(f"  first_seg_start: {first_seg_start} ({first_seg_start/60:.2f} hours)")
    print(f"  rep_start_min: {rep_start_min} ({rep_start_min/60:.2f} hours)")

    if rep_start_min < first_seg_start:
        rotate_idx = 0
        print(f"  Report starts BEFORE first segment → rotate_idx = 0")
    else:
        rotate_idx = len(seg_list_sorted) - 1
        print(f"  Report starts AFTER all segments → rotate_idx = {rotate_idx}")

print(f"\nFinal rotate_idx: {rotate_idx}")

seg_list_ordered = seg_list_sorted[rotate_idx:] + seg_list_sorted[:rotate_idx]
print(f"\nOrdered segments (after rotation):")
for i, seg in enumerate(seg_list_ordered):
    print(f"  {i}: {seg['start_time']}-{seg['end_time']}")

# Now simulate the normalization and overlap calculation
print(f"\n{'='*80}")
print("NORMALIZATION AND OVERLAP CALCULATION")
print(f"{'='*80}\n")

# Split the report into parts (before and after midnight)
parts = []
if rep_end_orig <= MINUTES_PER_DAY:
    parts.append(("same day", rep_start_orig, rep_end_orig))
else:
    parts.append(("day 1", rep_start_orig, MINUTES_PER_DAY))
    parts.append(("day 2", 0, rep_end_orig - MINUTES_PER_DAY))

print(f"Report parts:")
for part_name, p_start, p_end in parts:
    print(f"  {part_name}: {p_start}-{p_end} ({(p_end-p_start)/60:.2f} hours)")

# For each part, calculate overlap with normalized segments
for part_name, p_start, p_end in parts:
    print(f"\n{'---'*20}")
    print(f"Processing part: {part_name} ({p_start}-{p_end})")
    print(f"{'---'*20}")

    # Determine which day this belongs to (for display)
    CUTOFF = 480  # 08:00

    # Split part if it crosses 08:00 cutoff
    sub_parts = []
    if p_start < CUTOFF < p_end:
        sub_parts.append((p_start, CUTOFF))
        sub_parts.append((CUTOFF, p_end))
    else:
        sub_parts.append((p_start, p_end))

    for s_start, s_end in sub_parts:
        print(f"\n  Sub-part: {s_start}-{s_end}")
        print(f"    s_end <= CUTOFF? {s_end} <= {CUTOFF} = {s_end <= CUTOFF}")

        # Normalize segments
        last_s_end_norm = -1
        for seg in seg_list_ordered:
            orig_s_start, orig_s_end = span_minutes(seg["start_time"], seg["end_time"])

            # Make continuous
            if last_s_end_norm == -1:
                pass  # First segment, keep as is
            else:
                while orig_s_start < last_s_end_norm:
                    orig_s_start += MINUTES_PER_DAY
                    orig_s_end += MINUTES_PER_DAY

            last_s_end_norm = orig_s_end

            # Calculate overlap
            overlap = overlap_minutes(s_start, s_end, orig_s_start, orig_s_end)

            print(f"    Segment {seg['start_time']}-{seg['end_time']}:")
            print(f"      Normalized: {orig_s_start}-{orig_s_end}")
            print(f"      Overlap with {s_start}-{s_end}: {overlap} minutes")

            if overlap > 0:
                print(f"      → MATCHED! {overlap/60:.2f} hours of {seg['segment_type']}")

print(f"\n{'='*80}")
print("END OF SIMULATION")
print(f"{'='*80}")
