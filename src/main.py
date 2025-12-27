import argparse
import sys
import os

from infrastructure.node import Node

def main():
    parser = argparse.ArgumentParser(description='Distributed Consensus Node')
    parser.add_argument('--id', type=int, required=True, help='Node ID (1-5)')
    args = parser.parse_args()
    
    if args.id < 1 or args.id > 5:
        print("Error: Node ID must be between 1 and 5")
        sys.exit(1)
    
    node = Node(args.id)
    node.serve()

if __name__ == '__main__':
    main()
