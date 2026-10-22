"""Only BF/BS quote frames between 100ms decision ticks; retain HL and all books."""
def bucket_events(items):
    selected = set(); quotes = {}
    for i, (_, _, e) in enumerate(items):
        if e['kind'] == 'market' and e['venue'] in ['bf', 'bs'] and e['payload']['frameKind'] == 'quote':
            quotes.setdefault(e['venue'], []).append(i)
        else: selected.add(i)
    for indices in quotes.values():
        times = [items[i][2].get('source_ms') if items[i][2].get('source_ms') is not None else items[i][0] for i in indices]
        # Preserve both native out-of-order rejection and proxy's last arrival.
        if any(b < a for a, b in zip(times, times[1:])): selected.update(indices)
        else: selected.add(indices[-1])
    return [items[i] for i in sorted(selected)]
