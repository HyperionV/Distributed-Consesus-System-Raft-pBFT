#!/usr/bin/env python3
"""Fix imports in generated protobuf files."""
import os
import re
from pathlib import Path

def fix_grpc_imports(file_path):
    """Convert absolute imports to package imports in _grpc.py files."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace: import raft_pb2 as raft__pb2
    # With:    from generated import raft_pb2 as raft__pb2
    # Also handle: from . import raft_pb2 as raft__pb2 (if already modified)
    patterns = [
        (r'^import (\w+_pb2) as (\w+)$', r'from generated import \1 as \2'),
        (r'^from \. import (\w+_pb2) as (\w+)$', r'from generated import \1 as \2'),
        (r'^import (\w+_pb2)$', r'from generated import \1'),
        (r'^from \. import (\w+_pb2)$', r'from generated import \1'),
    ]
    
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        fixed_line = line
        for pattern, replacement in patterns:
            fixed_line = re.sub(pattern, replacement, fixed_line, flags=re.MULTILINE)
        fixed_lines.append(fixed_line)
    
    fixed_content = '\n'.join(fixed_lines)
    
    if fixed_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        print(f"Fixed imports in {file_path.name}")
        return True
    return False

def main():
    """Fix all _grpc.py files in src/generated/."""
    generated_dir = Path(__file__).parent.parent / 'src' / 'generated'
    
    if not generated_dir.exists():
        print(f"Error: {generated_dir} does not exist")
        return 1
    
    grpc_files = list(generated_dir.glob('*_grpc.py'))
    
    if not grpc_files:
        print(f"No *_grpc.py files found in {generated_dir}")
        return 1
    
    print(f"Fixing imports in {len(grpc_files)} files...")
    fixed_count = sum(fix_grpc_imports(f) for f in grpc_files)
    
    print(f"\nFixed {fixed_count}/{len(grpc_files)} files")
    return 0

if __name__ == '__main__':
    exit(main())
