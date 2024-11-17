import time
from collections import defaultdict
from typing import Optional
from contextlib import contextmanager

class TimerContext:
    def __init__(self):
        self.timings = defaultdict(list)
        self._start_times = {}
    
    @contextmanager
    def timer(self, section_name: str):
        """Context manager to time a block of code.
        
        Args:
            section_name: Name to identify this code section in the results
        """
        try:
            self.start(section_name)
            yield
        finally:
            self.stop(section_name)
    
    def start(self, section_name: str):
        """Start timing a section."""
        self._start_times[section_name] = time.perf_counter()
    
    def stop(self, section_name: str):
        """Stop timing a section and record the duration."""
        if section_name not in self._start_times:
            raise ValueError(f"Timer for {section_name} was never started")
        
        duration = time.perf_counter() - self._start_times[section_name]
        self.timings[section_name].append(duration)
        del self._start_times[section_name]
    
    def report(self, decimals: int = 4) -> None:
        """Print a summary report of all timings.
        
        Args:
            decimals: Number of decimal places to show in times
        """
        print("\n=== Timing Report ===")
        
        if not self.timings:
            print("No timings recorded.")
            return
        
        max_name_len = max(len(name) for name in self.timings)
        
        for name, times in self.timings.items():
            avg_time = sum(times) / len(times)
            total_time = sum(times)
            runs = len(times)
            
            print(f"\n{name:{max_name_len}} ({runs} runs)")
            print(f"{'Average:':{max_name_len}} {avg_time:.{decimals}f} seconds")
            print(f"{'Total:':{max_name_len}} {total_time:.{decimals}f} seconds")
            if runs > 1:
                print(f"{'Min:':{max_name_len}} {min(times):.{decimals}f} seconds")
                print(f"{'Max:':{max_name_len}} {max(times):.{decimals}f} seconds")