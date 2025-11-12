#!/usr/bin/env python3
"""Fix conflicts in tiki_lazada_elt_dag.py"""

import re

def fix_dag_conflicts():
    dag_file = r"c:\Đo an\airflow\dags\tiki_lazada_elt_dag.py"
    
    with open(dag_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Remove conflict markers and keep the newer version (after =======)
    # Find the start of conflict
    conflict_start = content.find('mmand=LAZADA_CMD.format(')
    if conflict_start == -1:
        print("No conflict found")
        return
    
    # Find the separator
    separator = content.find('=======')
    if separator == -1:
        print("No conflict separator found")
        return
    
    # Find the end marker
    end_marker = content.find('>>>>>>> ')
    if end_marker == -1:
        print("No end marker found")
        return
    
    # Keep everything before conflict and everything after end marker
    before_conflict = content[:conflict_start]
    after_conflict = content[content.find('\n', end_marker) + 1:]
    
    # Get the newer version (between ======= and >>>>>>>)
    newer_version = content[separator + 8:end_marker]
    
    # Combine the parts
    fixed_content = newer_version + after_conflict
    
    # Write back
    with open(dag_file, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    print("✅ Đã giải quyết conflict trong tiki_lazada_elt_dag.py")

if __name__ == "__main__":
    fix_dag_conflicts()