def amount_encoding(x: int, min_val: int, max_val: int) -> float:
    if x < min_val:
        return 0
    if x > max_val:
        return 1
    return (x - min_val) / (max_val - min_val)
