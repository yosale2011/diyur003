"""
Test the proposed fix for segment rotation logic
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


def find_rotation_index_OLD(seg_list_sorted, rep_start_min):
    """OLD logic (has bug)"""
    best_start_diff = -1
    rotate_idx = 0

    for i, seg in enumerate(seg_list_sorted):
        seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])
        if seg_start_min <= rep_start_min:
            if seg_start_min > best_start_diff:
                best_start_diff = seg_start_min
                rotate_idx = i

    if best_start_diff == -1 and seg_list_sorted:
        first_seg_start, _ = span_minutes(seg_list_sorted[0]["start_time"], seg_list_sorted[0]["end_time"])

        if rep_start_min < first_seg_start:
            rotate_idx = 0
        else:
            rotate_idx = len(seg_list_sorted) - 1

    return rotate_idx


def find_rotation_index_NEW(seg_list_sorted, rep_start_min):
    """NEW logic (proposed fix)"""
    best_start_diff = -1
    rotate_idx = 0

    # Define a threshold: segments starting before this time might be "next morning" segments
    # But only if they are much earlier than the report start time
    MORNING_CUTOFF = 480  # 08:00

    for i, seg in enumerate(seg_list_sorted):
        seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])

        # Check if this segment should be considered as starting "before" the report
        # A segment starting at 06:30 when report is at 15:00 is likely NEXT DAY, not before
        # But a segment starting at 08:00 when report is at 08:00 IS the start

        # Logic: If segment starts before MORNING_CUTOFF (08:00) AND report starts after 12:00,
        # then this segment is likely "next morning" and should not be considered as "before" report
        is_morning_segment = seg_start_min < MORNING_CUTOFF
        is_afternoon_report = rep_start_min >= 720  # 12:00

        if is_morning_segment and is_afternoon_report:
            # This is a morning segment (like 06:30) but report is in afternoon (like 15:00)
            # Skip this segment - it's not before the report, it's NEXT DAY
            continue

        if seg_start_min <= rep_start_min:
            if seg_start_min > best_start_diff:
                best_start_diff = seg_start_min
                rotate_idx = i

    # If we didn't find any segment, handle special cases
    if best_start_diff == -1 and seg_list_sorted:
        first_seg_start, _ = span_minutes(seg_list_sorted[0]["start_time"], seg_list_sorted[0]["end_time"])

        # Find first non-morning segment for afternoon reports
        if rep_start_min >= 720:  # Report is in afternoon
            first_afternoon_idx = None
            for i, seg in enumerate(seg_list_sorted):
                seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])
                if seg_start_min >= MORNING_CUTOFF:
                    first_afternoon_idx = i
                    break

            if first_afternoon_idx is not None:
                rotate_idx = first_afternoon_idx
            else:
                # All segments are morning - unusual, use first
                rotate_idx = 0
        else:
            # Report is in morning/early, use standard logic
            if rep_start_min < first_seg_start:
                rotate_idx = 0
            else:
                rotate_idx = len(seg_list_sorted) - 1

    return rotate_idx


# Test cases
test_cases = [
    {
        "name": "Avigel (15:00-08:00) - PROBLEM CASE",
        "report_start": "15:00",
        "segments": [
            {"start_time": "16:00", "end_time": "22:00"},
            {"start_time": "22:00", "end_time": "06:30"},
            {"start_time": "06:30", "end_time": "08:00"},
        ],
        "expected_first_segment": "16:00",  # Should start from 16:00, not 06:30
    },
    {
        "name": "Abramoff (16:00-08:00) - CORRECT CASE",
        "report_start": "16:00",
        "segments": [
            {"start_time": "16:00", "end_time": "22:00"},
            {"start_time": "22:00", "end_time": "06:30"},
            {"start_time": "06:30", "end_time": "08:00"},
        ],
        "expected_first_segment": "16:00",
    },
    {
        "name": "Tehila (16:00-08:00) - CORRECT CASE",
        "report_start": "16:00",
        "segments": [
            {"start_time": "16:00", "end_time": "22:00"},
            {"start_time": "22:00", "end_time": "06:30"},
            {"start_time": "06:30", "end_time": "08:00"},
        ],
        "expected_first_segment": "16:00",
    },
    {
        "name": "Day shift (08:00-16:00)",
        "report_start": "08:00",
        "segments": [
            {"start_time": "08:00", "end_time": "12:00"},
            {"start_time": "12:00", "end_time": "16:00"},
        ],
        "expected_first_segment": "08:00",
    },
    {
        "name": "Early morning shift (06:00-14:00)",
        "report_start": "06:00",
        "segments": [
            {"start_time": "06:00", "end_time": "10:00"},
            {"start_time": "10:00", "end_time": "14:00"},
        ],
        "expected_first_segment": "06:00",
    },
    {
        "name": "Late start (14:00-08:00)",
        "report_start": "14:00",
        "segments": [
            {"start_time": "16:00", "end_time": "22:00"},
            {"start_time": "22:00", "end_time": "06:30"},
            {"start_time": "06:30", "end_time": "08:00"},
        ],
        "expected_first_segment": "16:00",  # Even earlier start, still should use 16:00
    },
    {
        "name": "Evening shift (18:00-22:00)",
        "report_start": "18:00",
        "segments": [
            {"start_time": "18:00", "end_time": "22:00"},
        ],
        "expected_first_segment": "18:00",
    },
    {
        "name": "Night shift starting at 22:00",
        "report_start": "22:00",
        "segments": [
            {"start_time": "22:00", "end_time": "06:00"},
        ],
        "expected_first_segment": "22:00",
    },
]

print("="*100)
print("TESTING OLD vs NEW ROTATION LOGIC")
print("="*100)

for test in test_cases:
    print(f"\n{'-'*100}")
    print(f"Test: {test['name']}")
    print(f"Report starts: {test['report_start']}")
    print(f"-"*100)

    # Sort segments
    seg_list_sorted = sorted(test["segments"], key=lambda s: span_minutes(s["start_time"], s["end_time"])[0])

    print(f"\nSorted segments:")
    for i, seg in enumerate(seg_list_sorted):
        print(f"  {i}: {seg['start_time']}-{seg['end_time']}")

    rep_start_min, _ = span_minutes(test["report_start"], "00:00")
    rep_start_min = rep_start_min % MINUTES_PER_DAY

    # Test OLD logic
    old_idx = find_rotation_index_OLD(seg_list_sorted, rep_start_min)
    old_ordered = seg_list_sorted[old_idx:] + seg_list_sorted[:old_idx]
    old_first = old_ordered[0]["start_time"]

    # Test NEW logic
    new_idx = find_rotation_index_NEW(seg_list_sorted, rep_start_min)
    new_ordered = seg_list_sorted[new_idx:] + seg_list_sorted[:new_idx]
    new_first = new_ordered[0]["start_time"]

    print(f"\nOLD logic:")
    print(f"  rotate_idx: {old_idx}")
    print(f"  First segment: {old_first}")
    print(f"  Ordered: {[s['start_time'] for s in old_ordered]}")

    print(f"\nNEW logic:")
    print(f"  rotate_idx: {new_idx}")
    print(f"  First segment: {new_first}")
    print(f"  Ordered: {[s['start_time'] for s in new_ordered]}")

    print(f"\nExpected first segment: {test['expected_first_segment']}")

    old_correct = (old_first == test['expected_first_segment'])
    new_correct = (new_first == test['expected_first_segment'])

    print(f"OLD logic result: {'PASS' if old_correct else 'FAIL'}")
    print(f"NEW logic result: {'PASS' if new_correct else 'FAIL'}")

    if not old_correct and new_correct:
        print(">>> FIX SUCCESSFUL! New logic fixes the problem.")
    elif old_correct and not new_correct:
        print(">>> REGRESSION! New logic breaks working case.")
    elif old_correct and new_correct:
        print(">>> Both work correctly.")
    else:
        print(">>> Both have issues.")

print(f"\n{'='*100}")
print("END OF TESTS")
print(f"{'='*100}")
